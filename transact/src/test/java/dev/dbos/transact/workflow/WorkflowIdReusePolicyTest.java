package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.EnqueueOptions;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.exceptions.DBOSWorkflowIdInUseException;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLTransientException;
import java.time.Duration;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

interface ReuseService {
  String quick(String value);

  String fail();

  String waitForMessage();

  /** Starts {@code childId} with REJECT: {@code how} is "start", "enqueue" or "call" (directly). */
  String startChild(String childId, String how);

  String startSameChildTwice(String childId);
}

class ReuseServiceImpl implements ReuseService {
  private final DBOS dbos;
  ReuseService proxy;

  ReuseServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String quick(String value) {
    return value;
  }

  @Override
  @Workflow
  public String fail() {
    throw new IllegalStateException("failed");
  }

  @Override
  @Workflow
  public String waitForMessage() {
    return dbos.<String>recv("go", Duration.ofMinutes(1)).orElse("timed out");
  }

  @Override
  @Workflow
  public String startChild(String childId, String how) {
    try {
      if (how.equals("call")) {
        try (var _o =
            new WorkflowOptions(childId)
                .withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT)
                .setContext()) {
          return proxy.quick("child");
        }
      }
      var options =
          new StartWorkflowOptions()
              .withWorkflowId(childId)
              .withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
      if (how.equals("enqueue")) {
        options = options.withQueue(WorkflowIdReusePolicyTest.QUEUE);
      }
      return dbos.startWorkflow(() -> proxy.quick("child"), options).getResult();
    } catch (DBOSWorkflowIdInUseException e) {
      return "in use: " + e.workflowStatus();
    }
  }

  @Override
  @Workflow
  public String startSameChildTwice(String childId) {
    var options =
        new StartWorkflowOptions()
            .withWorkflowId(childId)
            .withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
    var first = dbos.startWorkflow(() -> proxy.quick("child"), options).getResult();
    try {
      dbos.startWorkflow(() -> proxy.quick("again"), options);
      return first + ", attached";
    } catch (DBOSWorkflowIdInUseException e) {
      return first + ", in use: " + e.workflowStatus();
    }
  }
}

/**
 * {@link WorkflowIdReusePolicy#REJECT} refuses a start or enqueue whose workflow ID is already
 * taken, whatever the holder's status, and writes nothing. {@link
 * WorkflowIdReusePolicy#RETURN_EXISTING}, the default, attaches to the holder as before.
 */
public class WorkflowIdReusePolicyTest {

  static final String QUEUE = "reuse-policy-queue";
  static final String SINGLE_SLOT_QUEUE = "reuse-policy-single-slot";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOS dbos;
  ReuseService proxy;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
    dbos = new DBOS(pgContainer.dbosConfig());
    var impl = new ReuseServiceImpl(dbos);
    proxy = dbos.registerProxy(ReuseService.class, impl);
    impl.proxy = proxy;
    dbos.launch();
    dbos.registerQueue(QUEUE, new QueueOptions());
    dbos.registerQueue(SINGLE_SLOT_QUEUE, new QueueOptions().withConcurrency(1));
  }

  private static StartWorkflowOptions withId(String workflowId) {
    return new StartWorkflowOptions().withWorkflowId(workflowId);
  }

  private static StartWorkflowOptions rejecting(String workflowId) {
    return withId(workflowId).withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
  }

  /**
   * Starts a workflow holding {@code workflowId} that is left in {@code state}, and returns what
   * lets it, or whatever it waits behind, finish.
   */
  private Runnable holdId(String workflowId, WorkflowState state) throws Exception {
    switch (state) {
      case SUCCESS ->
          dbos.startWorkflow(() -> proxy.quick("first"), withId(workflowId)).getResult();
      case ERROR -> {
        var handle = dbos.startWorkflow(() -> proxy.fail(), withId(workflowId));
        assertThrows(IllegalStateException.class, handle::getResult);
      }
      case PENDING -> {
        dbos.startWorkflow(() -> proxy.waitForMessage(), withId(workflowId));
        return () -> dbos.send(workflowId, "done", "go");
      }
      case CANCELLED -> {
        dbos.startWorkflow(() -> proxy.waitForMessage(), withId(workflowId));
        dbos.cancelWorkflow(workflowId);
      }
      case DELAYED ->
          dbos.startWorkflow(
              () -> proxy.quick("first"),
              withId(workflowId).withQueue(QUEUE).withDelay(Duration.ofHours(1)));
      case ENQUEUED -> {
        // Behind a blocker holding the queue's only slot.
        var blockerId = UUID.randomUUID().toString();
        dbos.startWorkflow(
            () -> proxy.waitForMessage(), withId(blockerId).withQueue(SINGLE_SLOT_QUEUE));
        dbos.startWorkflow(
            () -> proxy.quick("first"), withId(workflowId).withQueue(SINGLE_SLOT_QUEUE));
        return () -> dbos.send(blockerId, "done", "go");
      }
      default -> throw new IllegalArgumentException(state.name());
    }
    return () -> {};
  }

  private EnqueueOptions enqueueQuick(String workflowId) {
    return new EnqueueOptions("quick", ReuseServiceImpl.class.getName(), QueueName.of(QUEUE))
        .withWorkflowId(workflowId);
  }

  @Test
  void aRejectingStartOnAFreshIdRuns() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    assertEquals(
        "fresh", dbos.startWorkflow(() -> proxy.quick("fresh"), rejecting(workflowId)).getResult());
  }

  @ParameterizedTest
  @EnumSource(
      value = WorkflowState.class,
      names = {"PENDING", "ENQUEUED", "DELAYED", "SUCCESS", "ERROR", "CANCELLED"})
  void aRejectingStartOnATakenIdIsRefusedAndWritesNothing(WorkflowState holderState)
      throws Exception {
    var workflowId = UUID.randomUUID().toString();
    var release = holdId(workflowId, holderState);
    var before = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertEquals(holderState.name(), before.status());

    var refused =
        assertThrows(
            DBOSWorkflowIdInUseException.class,
            () -> dbos.startWorkflow(() -> proxy.quick("second"), rejecting(workflowId)));

    assertEquals(workflowId, refused.workflowId());
    assertEquals(holderState, refused.workflowStatus());
    assertEquals(before.workflowName(), refused.workflowName());
    var after = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertEquals(before.inputs(), after.inputs(), "the holder's input is untouched");
    assertEquals(before.updatedAt(), after.updatedAt(), "the holder's row is untouched");
    release.run();
  }

  // The ID is checked before the workflow's name, so a call naming another workflow is still
  // refused as in use rather than as a conflicting workflow.
  @Test
  void aRejectingStartNamingAnotherWorkflowIsRefusedAsInUse() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    var refused =
        assertThrows(
            DBOSWorkflowIdInUseException.class,
            () -> dbos.startWorkflow(() -> proxy.fail(), rejecting(workflowId)));
    assertEquals("quick", refused.workflowName());
  }

  // A deduplication ID collides only for a new workflow ID: the same ID is in use, whatever its
  // deduplication ID, and another ID meets the deduplication collision.
  @Test
  void aRejectingEnqueueIsRefusedByWhicheverItCollidesWith() throws Exception {
    var holderId = UUID.randomUUID().toString();
    var deduplicationId = UUID.randomUUID().toString();
    // Delayed, so the holder keeps its deduplication ID.
    dbos.startWorkflow(
        () -> proxy.quick("first"),
        withId(holderId)
            .withQueue(QUEUE)
            .withDeduplicationId(deduplicationId)
            .withDelay(Duration.ofHours(1)));

    assertThrows(
        DBOSWorkflowIdInUseException.class,
        () ->
            dbos.startWorkflow(
                () -> proxy.quick("second"),
                rejecting(holderId).withQueue(QUEUE).withDeduplicationId(deduplicationId)));
    var otherId = UUID.randomUUID().toString();
    assertThrows(
        DBOSQueueDuplicatedException.class,
        () ->
            dbos.startWorkflow(
                () -> proxy.quick("second"),
                rejecting(otherId).withQueue(QUEUE).withDeduplicationId(deduplicationId)));
  }

  // A call whose insert committed but whose acknowledgement was lost is retried with the same
  // creator token, finds its own row, and is not refused.
  @ParameterizedTest
  @ValueSource(strings = {"start", "enqueue", "client"})
  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT)
  void aRejectingCallRetriedAfterItsCommitIsNotRefused(String via) throws Exception {
    var workflowId = UUID.randomUUID().toString();
    var lostAck =
        new DebugTriggers.DebugAction().setSqlExceptionToThrow(new SQLTransientException());
    DebugTriggers.setDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT, lostAck);
    try {
      var rejecting =
          enqueueQuick(workflowId).withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
      switch (via) {
        case "start" ->
            assertEquals(
                "x", dbos.startWorkflow(() -> proxy.quick("x"), rejecting(workflowId)).getResult());
        case "enqueue" ->
            assertEquals("x", dbos.enqueueWorkflow(rejecting, new Object[] {"x"}).getResult());
        case "client" -> {
          try (var client = pgContainer.dbosClient()) {
            assertEquals("x", client.enqueueWorkflow(rejecting, new Object[] {"x"}).getResult());
          }
        }
        default -> throw new IllegalArgumentException(via);
      }
    } finally {
      DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT);
    }
    assertNull(lostAck.getSqlExceptionToThrow(), "the commit's acknowledgement was lost");
  }

  @Test
  void aRejectingEnqueueOnATakenIdIsRefused() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    assertThrows(
        DBOSWorkflowIdInUseException.class,
        () ->
            dbos.startWorkflow(
                () -> proxy.quick("second"), rejecting(workflowId).withQueue(QUEUE)));
  }

  @Test
  void aRejectingRuntimeEnqueueWithOptionsOnATakenIdIsRefused() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    var options = enqueueQuick(workflowId).withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
    assertThrows(
        DBOSWorkflowIdInUseException.class,
        () -> dbos.enqueueWorkflow(options, new Object[] {"second"}));
  }

  @Test
  void aRejectingClientEnqueueOnATakenIdIsRefused() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    try (var client = pgContainer.dbosClient()) {
      var options =
          new EnqueueOptions("quick", QueueName.of(QUEUE))
              .withWorkflowId(workflowId)
              .withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
      var refused =
          assertThrows(
              DBOSWorkflowIdInUseException.class,
              () -> client.enqueueWorkflow(options, new Object[] {"second"}));
      assertEquals(WorkflowState.SUCCESS, refused.workflowStatus());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void returnExistingAttachesToTheHolder(boolean explicit) throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    var options =
        explicit
            ? withId(workflowId).withWorkflowIdReusePolicy(WorkflowIdReusePolicy.RETURN_EXISTING)
            : withId(workflowId);
    assertEquals("first", dbos.startWorkflow(() -> proxy.quick("second"), options).getResult());
  }

  private static WorkflowOptions.Guard rejectingBlock(String workflowId) {
    return new WorkflowOptions(workflowId)
        .withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT)
        .setContext();
  }

  @Test
  void aWorkflowOptionsPolicyRefusesADirectCallOnATakenId() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    var before = DBUtils.getWorkflowRow(dataSource, workflowId);

    try (var _o = rejectingBlock(workflowId)) {
      var refused = assertThrows(DBOSWorkflowIdInUseException.class, () -> proxy.quick("second"));
      assertEquals(WorkflowState.SUCCESS, refused.workflowStatus());
    }
    var after = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertEquals(before.inputs(), after.inputs(), "the holder's input is untouched");
    assertEquals(before.updatedAt(), after.updatedAt(), "the holder's row is untouched");
  }

  @Test
  void aWorkflowOptionsPolicyLetsADirectCallOnAFreshIdRun() throws Exception {
    try (var _o = rejectingBlock(UUID.randomUUID().toString())) {
      assertEquals("fresh", proxy.quick("fresh"));
    }
  }

  // A start or enqueue that takes the block's ID takes its policy too.
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void aWorkflowOptionsPolicyRefusesAStartTakingItsId(boolean queued) throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    var options = queued ? new StartWorkflowOptions().withQueue(QUEUE) : new StartWorkflowOptions();

    try (var _o = rejectingBlock(workflowId)) {
      assertThrows(
          DBOSWorkflowIdInUseException.class,
          () -> dbos.startWorkflow(() -> proxy.quick("second"), options));
    }
  }

  @Test
  void aWorkflowOptionsPolicyRefusesAnEnqueueTakingItsId() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    var options =
        new EnqueueOptions("quick", ReuseServiceImpl.class.getName(), QueueName.of(QUEUE));

    try (var _o = rejectingBlock(workflowId)) {
      assertThrows(
          DBOSWorkflowIdInUseException.class,
          () -> dbos.enqueueWorkflow(options, new Object[] {"second"}));
    }
  }

  // The policy goes with the block's ID: a call giving its own ID, and a nested block setting
  // another ID, are not governed by it, and the workflow that takes the block's ID is.
  @Test
  void aWorkflowOptionsPolicyAppliesOnlyToTheWorkflowTakingItsId() throws Exception {
    var takenId = UUID.randomUUID().toString();
    holdId(takenId, WorkflowState.SUCCESS);

    try (var _o = rejectingBlock(UUID.randomUUID().toString())) {
      assertEquals(
          "first", dbos.startWorkflow(() -> proxy.quick("second"), withId(takenId)).getResult());

      try (var _i = new WorkflowOptions(takenId).setContext()) {
        assertEquals("first", proxy.quick("second"));
      }

      assertEquals("fresh", proxy.quick("fresh"));
    }
  }

  @Test
  void aWorkflowOptionsPolicyNeedsAWorkflowId() {
    var options = new WorkflowOptions().withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
    assertThrows(IllegalArgumentException.class, options::setContext);
  }

  @Test
  void aDebounceRefusesAWorkflowOptionsRejectPolicy() {
    try (var _o = rejectingBlock(UUID.randomUUID().toString())) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              dbos.<String>debouncer()
                  .debounce("key", Duration.ofSeconds(1), () -> proxy.quick("x")));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"start", "enqueue", "call"})
  void aRefusedChildIsRecordedAtItsParentsStepAndReplayed(String how) throws Exception {
    var childId = UUID.randomUUID().toString();
    holdId(childId, WorkflowState.SUCCESS);
    var parentId = UUID.randomUUID().toString();

    assertEquals(
        "in use: SUCCESS",
        dbos.startWorkflow(() -> proxy.startChild(childId, how), withId(parentId)).getResult());

    var steps = dbos.listWorkflowSteps(parentId);
    assertEquals(1, steps.size());
    assertNotNull(steps.get(0).error(), "the refusal is recorded at the parent's step");
    assertTrue(
        steps.get(0).error().className().endsWith("DBOSWorkflowIdInUseException"),
        steps.get(0).error().className());

    // A replay throws the recorded refusal, even once the ID is free again.
    dbos.deleteWorkflow(childId);
    assertEquals(
        "in use: SUCCESS", dbos.<String, RuntimeException>forkWorkflow(parentId, 1).getResult());
  }

  // A parent's first start creates the child; its second, under the same ID, is refused, and a
  // replay of the parent attaches to the first and throws the recorded refusal for the second.
  @Test
  void aParentStartingTheSameChildTwiceIsRefusedTheSecondTime() throws Exception {
    var childId = UUID.randomUUID().toString();
    var parentId = UUID.randomUUID().toString();
    var expected = "child, in use: SUCCESS";
    assertEquals(
        expected,
        dbos.startWorkflow(() -> proxy.startSameChildTwice(childId), withId(parentId)).getResult());

    var steps = dbos.listWorkflowSteps(parentId);
    assertEquals(
        expected, dbos.<String, RuntimeException>forkWorkflow(parentId, steps.size()).getResult());
  }
}
