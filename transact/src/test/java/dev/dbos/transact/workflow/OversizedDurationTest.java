package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.PgContainer;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

interface DurationService {
  String sleepTooLongThenBriefly();

  String sleepPastTheLatestInstant();

  String recvWithTimeout(boolean tooLongForMillis);

  String setEvent();

  String enqueueChildPastTheLatestInstant();

  String child();
}

class DurationServiceImpl implements DurationService {
  private final DBOS dbos;
  DurationService proxy;

  DurationServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String sleepTooLongThenBriefly() {
    try {
      dbos.sleep(Duration.ofSeconds(Long.MAX_VALUE));
      return "slept";
    } catch (IllegalArgumentException e) {
      dbos.sleep(Duration.ofMillis(1));
      return e.getMessage();
    }
  }

  @Override
  @Workflow
  public String sleepPastTheLatestInstant() {
    dbos.sleep(Duration.ofMillis(Long.MAX_VALUE));
    return "woke";
  }

  @Override
  @Workflow
  public String recvWithTimeout(boolean tooLongForMillis) {
    var timeout =
        tooLongForMillis ? Duration.ofSeconds(Long.MAX_VALUE) : Duration.ofMillis(Long.MAX_VALUE);
    return dbos.<String>recv("topic", timeout).orElse("timed out");
  }

  @Override
  @Workflow
  public String setEvent() {
    dbos.setEvent("key", "value");
    return "set";
  }

  @Override
  @Workflow
  public String enqueueChildPastTheLatestInstant() {
    try {
      dbos.startWorkflow(
          () -> proxy.child(),
          new StartWorkflowOptions()
              .withQueue(OversizedDurationTest.QUEUE)
              .withDelay(Duration.ofMillis(Long.MAX_VALUE)));
      return "enqueued";
    } catch (IllegalArgumentException e) {
      return e.getClass().getSimpleName();
    }
  }

  @Override
  @Workflow
  public String child() {
    return "child";
  }
}

/**
 * Every duration DBOS records is stored as epoch milliseconds in a 64-bit integer. One too long to
 * convert is refused before anything is recorded. One that converts but carries the clock past the
 * largest instant is refused where it is a time to act at -- a wake time, a release time -- and
 * recorded so a replay refuses it again, and held at the largest instant where it only bounds a
 * wait, which then never times out.
 */
public class OversizedDurationTest {

  static final String QUEUE = "oversized-duration-queue";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  DurationService proxy;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    var impl = new DurationServiceImpl(dbos);
    proxy = dbos.registerProxy(DurationService.class, impl);
    impl.proxy = proxy;
    dbos.launch();
    dbos.registerQueue(QUEUE, new QueueOptions());
  }

  @Test
  void optionsRefuseADurationTooLongForMilliseconds() {
    var tooLong = Duration.ofSeconds(Long.MAX_VALUE);
    assertThrows(
        IllegalArgumentException.class, () -> new StartWorkflowOptions().withTimeout(tooLong));
    assertThrows(
        IllegalArgumentException.class, () -> new StartWorkflowOptions().withDelay(tooLong));
    assertThrows(
        IllegalArgumentException.class, () -> new ForkOptions(null, null, tooLong, null, null));
    assertThrows(IllegalArgumentException.class, () -> new WorkflowDelay.Delay(tooLong));

    // The largest duration that converts is accepted.
    var largest = Duration.ofMillis(Long.MAX_VALUE);
    assertDoesNotThrow(() -> new StartWorkflowOptions().withTimeout(largest));
    assertDoesNotThrow(() -> new StartWorkflowOptions().withDelay(largest));
    assertDoesNotThrow(() -> new ForkOptions(null, null, largest, null, null));
    assertDoesNotThrow(() -> new WorkflowDelay.Delay(largest));
  }

  @Test
  void aSleepTooLongForMillisecondsIsRefusedBeforeItTakesAStep() {
    assertEquals(
        "sleep duration must fit in milliseconds as a 64-bit integer",
        proxy.sleepTooLongThenBriefly());
    // The refused sleep took no step id: the brief sleep after it is step 0.
    var steps = dbos.listWorkflowSteps(lastWorkflowId("sleepTooLongThenBriefly"));
    assertEquals(1, steps.size());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("DBOS.sleep", steps.get(0).functionName());
  }

  @Test
  void aSleepPastTheLatestInstantIsRefusedAndTheRefusalReplayed() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    var handle =
        dbos.startWorkflow(
            () -> proxy.sleepPastTheLatestInstant(),
            new StartWorkflowOptions().withWorkflowId(workflowId));
    var refused = assertThrows(IllegalArgumentException.class, handle::getResult);
    assertTrue(refused.getMessage().contains("past the latest instant"), refused.getMessage());

    var steps = dbos.listWorkflowSteps(workflowId);
    assertEquals(1, steps.size());
    assertEquals("DBOS.sleep", steps.get(0).functionName());
    assertNotNull(steps.get(0).error(), "the refusal is recorded as the sleep's outcome");

    // A replay throws the recorded refusal rather than computing the wake time again.
    var forked = dbos.<String, RuntimeException>forkWorkflow(workflowId, 1);
    var replayed = assertThrows(IllegalArgumentException.class, forked::getResult);
    assertEquals(refused.getMessage(), replayed.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void aReceiveWhoseTimeoutIsTooFarOutToStoreNeverTimesOut(boolean tooLongForMillis)
      throws Exception {
    var workflowId = UUID.randomUUID().toString();
    var handle =
        dbos.startWorkflow(
            () -> proxy.recvWithTimeout(tooLongForMillis),
            new StartWorkflowOptions().withWorkflowId(workflowId));

    // Wait for the deadline to be recorded, so a wait that had already timed out would show.
    long giveUp = System.currentTimeMillis() + 10_000;
    StepInfo deadline = null;
    while (deadline == null) {
      assertTrue(System.currentTimeMillis() < giveUp, "the receive never recorded its deadline");
      deadline =
          dbos.listWorkflowSteps(workflowId).stream()
              .filter(step -> step.functionName().equals("DBOS.sleep"))
              .findFirst()
              .orElse(null);
      Thread.sleep(50);
    }
    assertEquals(Long.MAX_VALUE, deadline.output(), "the deadline is held at the largest instant");

    dbos.send(workflowId, "hello", "topic");
    assertEquals("hello", handle.getResult());
  }

  @Test
  void aGetEventOutsideAWorkflowWithATimeoutTooFarOutToStoreWaits() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    dbos.startWorkflow(
            () -> proxy.setEvent(), new StartWorkflowOptions().withWorkflowId(workflowId))
        .getResult();
    assertEquals(
        "value",
        dbos.<String>getEvent(workflowId, "key", Duration.ofSeconds(Long.MAX_VALUE)).orElseThrow());
  }

  @Test
  void aDelayPastTheLatestInstantIsRefusedAtEnqueue() {
    var workflowId = UUID.randomUUID().toString();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.startWorkflow(
                () -> proxy.child(),
                new StartWorkflowOptions()
                    .withWorkflowId(workflowId)
                    .withQueue(QUEUE)
                    .withDelay(Duration.ofMillis(Long.MAX_VALUE))));
    assertTrue(dbos.getWorkflowStatus(workflowId).isEmpty(), "nothing is written");
  }

  @Test
  void aChildRefusedForItsDelayIsRecordedAtItsParentsStep() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    var refusal = "DelayPastLatestInstantException";
    assertEquals(
        refusal,
        dbos.startWorkflow(
                () -> proxy.enqueueChildPastTheLatestInstant(),
                new StartWorkflowOptions().withWorkflowId(workflowId))
            .getResult());

    var steps = dbos.listWorkflowSteps(workflowId);
    assertEquals(1, steps.size());
    assertNotNull(steps.get(0).error(), "the refusal is recorded at the parent's step");
    assertTrue(
        steps.get(0).error().className().endsWith(refusal), steps.get(0).error().className());

    // A replay throws the recorded refusal rather than deciding again.
    assertEquals(refusal, dbos.<String, RuntimeException>forkWorkflow(workflowId, 1).getResult());
  }

  private String lastWorkflowId(String workflowName) {
    return dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName(workflowName))
        .get(0)
        .workflowId();
  }
}
