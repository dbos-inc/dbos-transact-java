package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;

import java.sql.SQLException;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class SchedulerServiceTest {

  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    SchedulerServiceTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    // let scheduled workflows drain
    Thread.sleep(1000);
    DBOS.shutdown();
  }

  @Test
  public void simpleScheduledWorkflow() throws Exception {

    var impl = new SkedServiceImpl();
    var q = new Queue("q2").withConcurrency(1);
    DBOS.registerQueue(q);
    DBOS.registerWorkflows(SkedService.class, impl);

    DBOS.launch();
    var schedulerService = DBOSTestAccess.getSchedulerService();

    // Run all sched WFs for 5 seconds(ish)
    Thread.sleep(5000);
    schedulerService.dbosShutDown();
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

    var workflows = DBOS.listWorkflows(new ListWorkflowsInput().withWorkflowName("withSteps"));
    assertTrue(workflows.size() <= 2);
    assertEquals(Constants.DBOS_INTERNAL_QUEUE, workflows.get(0).queueName());

    var steps = DBOS.listWorkflowSteps(workflows.get(0).workflowId());
    assertEquals(2, steps.size());

    var q2workflows = DBOS.listWorkflows(new ListWorkflowsInput().withWorkflowName("everyThird"));
    assertTrue(q2workflows.size() >= 1);
    assertEquals("q2", q2workflows.get(0).queueName());

    // See about makeup work (ignore missed)
    var timeToSleep = 5000 - (System.currentTimeMillis() - timeAsOfShutdown);
    Thread.sleep(timeToSleep < 0 ? 0 : timeToSleep);
    schedulerService.dbosLaunched();
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
  public void invalidSignature() {
    var e =
        assertThrows(
            IllegalArgumentException.class,
            () -> DBOS.registerWorkflows(InvalidSig.class, new InvalidSigImpl()));
    assertEquals(
        "Invalid signature for Scheduled workflow dev.dbos.transact.scheduled.InvalidSigImpl//scheduledWF. Signature must be (Instant, Instant)",
        e.getMessage());
  }

  @Test
  public void invalidCron() {
    var e =
        assertThrows(
            IllegalArgumentException.class,
            () -> DBOS.registerWorkflows(InvalidCron.class, new InvalidCronImpl()));
    assertEquals("Cron expression contains 5 parts but we expect one of [6]", e.getMessage());
  }
}
