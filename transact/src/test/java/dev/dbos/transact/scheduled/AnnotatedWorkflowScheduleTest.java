package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;

import java.time.Duration;
import java.time.OffsetDateTime;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotatedWorkflowScheduleTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();
  }

  @Test
  public void simpleScheduledWorkflow() throws Exception {

    var impl = new AnnotatedScheduledServiceImpl(dbos);
    var q = new Queue("q2").withConcurrency(1);
    dbos.registerQueue(q);
    dbos.registerProxy(AnnotatedScheduledService.class, impl);

    dbos.launch();
    var schedulerService = DBOSTestAccess.getSchedulerService(dbos);

    // Run all sched WFs for 5 seconds(ish)
    Thread.sleep(5000);
    schedulerService.close();
    var timeAsOfShutdown = System.currentTimeMillis();
    Thread.sleep(1000);

    // All checks for all WFs
    int count1 = impl.everySecondCounter;
    System.out.println("Final count (1s): " + count1);
    assertTrue(count1 >= 3);
    assertTrue(count1 <= 6); // Flaky, have seen 6

    int count1im = impl.everySecondCounterIgnoreMissed;
    System.out.println("Final count (1s ignore missed): " + count1im);
    assertTrue(count1im >= 3);
    assertTrue(count1im <= 6);

    int count1dim = impl.everySecondCounterDontIgnoreMissed;
    System.out.println("Final count (1s do not ignore missed): " + count1dim);
    assertTrue(count1dim >= 3);
    assertTrue(count1dim <= 6);

    int count3 = impl.everyThirdCounter;
    System.out.println("Final count (3s): " + count3);
    assertTrue(count3 >= 1);
    assertTrue(count3 <= 2);

    assertNotNull(impl.scheduled);
    assertNotNull(impl.actual);
    Duration delta = Duration.between(impl.scheduled, impl.actual).abs();
    assertTrue(delta.toMillis() < 1000);

    var workflows = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("withSteps"));
    assertTrue(workflows.size() <= 2);
    assertEquals(Constants.DBOS_INTERNAL_QUEUE, workflows.get(0).queueName());

    var steps = dbos.listWorkflowSteps(workflows.get(0).workflowId());
    assertEquals(2, steps.size());

    var q2workflows = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("everyThird"));
    assertTrue(q2workflows.size() >= 1);
    assertEquals("q2", q2workflows.get(0).queueName());

    dbos.shutdown();

    // See about makeup work (ignore missed)
    var timeToSleep = 5000 - (System.currentTimeMillis() - timeAsOfShutdown);
    Thread.sleep(timeToSleep < 0 ? 0 : timeToSleep);
    dbos.launch();
    Thread.sleep(2000);

    int count1imb = impl.everySecondCounterIgnoreMissed;
    System.out.println("Final count (1s ignore missed, after resume): " + count1imb);
    assertTrue(count1imb >= 4);
    assertTrue(count1imb <= 9);

    int count1dimb = impl.everySecondCounterDontIgnoreMissed;
    System.out.println("Final count (1s do not ignore missed, after resume): " + count1dimb);
    assertTrue(count1dimb >= 10);
    assertTrue(count1dimb <= 14);
  }

  @Test
  public void annotatedWorkflowIdUsesOffsetDateTimeFormat() throws Exception {
    // Annotated scheduler IDs must use OffsetDateTime, not ZonedDateTime with zone brackets.
    var q = new Queue("q2").withConcurrency(1);
    dbos.registerQueue(q);
    var impl = new AnnotatedScheduledServiceImpl(dbos);
    dbos.registerProxy(AnnotatedScheduledService.class, impl);
    dbos.launch();

    Thread.sleep(3000);
    DBOSTestAccess.getSchedulerService(dbos).close();

    var workflows = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("everySecond"));
    assertFalse(workflows.isEmpty(), "Expected at least one everySecond execution");
    for (var wf : workflows) {
      var id = wf.workflowId();
      assertFalse(id.contains("["), "Annotated workflow ID must not contain zone brackets: " + id);
      // ID format: sched-{name}/{class}/-{offsetDateTime}
      // The fully-qualified name ends with '/', so the timestamp starts two chars after the
      // last '/' (skipping the trailing '-' separator).
      var suffix = id.substring(id.lastIndexOf('/') + 2);
      assertDoesNotThrow(
          () -> OffsetDateTime.parse(suffix), "Date suffix must be a valid OffsetDateTime: " + id);
    }
  }

  @Test
  public void invalidSignature() {
    var e =
        assertThrows(
            IllegalArgumentException.class,
            () -> dbos.registerProxy(InvalidSig.class, new InvalidSigImpl()));
    assertEquals(
        "Invalid signature for annotated workflow schedule scheduledWF/dev.dbos.transact.scheduled.InvalidSigImpl/. Signature must be (Instant, Instant)",
        e.getMessage());
  }

  @Test
  public void invalidCron() {
    var e =
        assertThrows(
            IllegalArgumentException.class,
            () -> dbos.registerProxy(InvalidCron.class, new InvalidCronImpl()));
    assertEquals("Cron expression contains 5 parts but we expect one of [6]", e.getMessage());
  }
}
