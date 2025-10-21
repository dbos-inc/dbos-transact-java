package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
class SchedulerServiceTest {

  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    SchedulerServiceTest.dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(2)
            .build();
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
    DBOS.registerWorkflows(SkedService.class, impl);

    DBOS.launch();
    var schedulerService = DBOSTestAccess.getSchedulerService();

    Thread.sleep(5000);
    schedulerService.stop();
    Thread.sleep(1000);

    int count = impl.everySecondCounter;
    System.out.println("Final count: " + count);
    assertTrue(count >= 2);
    assertTrue(count <= 5);
  }

  @Test
  public void ThirdSecWorkflow() throws Exception {

    var impl = new SkedServiceImpl();
    DBOS.registerWorkflows(SkedService.class, impl);

    DBOS.launch();
    var schedulerService = DBOSTestAccess.getSchedulerService();

    Thread.sleep(5000);
    schedulerService.stop();
    Thread.sleep(1000);

    int count = impl.everyThirdCounter;
    System.out.println("Final count: " + count);
    assertTrue(count >= 1);
    assertTrue(count <= 2);
  }

  @Test
  public void MultipleWorkflowsTest() throws Exception {

    var impl = new SkedServiceImpl();
    DBOS.registerWorkflows(SkedService.class, impl);
    DBOS.launch();
    var schedulerService = DBOSTestAccess.getSchedulerService();

    Thread.sleep(5000);
    schedulerService.stop();
    Thread.sleep(1000);

    int count = impl.everySecondCounter;
    System.out.println("Final count: " + count);
    assertTrue(count >= 2);
    assertTrue(count <= 5);
    int count3 = impl.everyThirdCounter;
    System.out.println("Final count3: " + count3);
    assertTrue(count3 <= 2);
  }

  @Test
  public void TimedWorkflowsTest() throws Exception {

    var impl = new SkedServiceImpl();
    DBOS.registerWorkflows(SkedService.class, impl);
    DBOS.launch();
    var schedulerService = DBOSTestAccess.getSchedulerService();

    Thread.sleep(5000);
    schedulerService.stop();
    Thread.sleep(1000);

    assertNotNull(impl.scheduled);
    assertNotNull(impl.actual);
    Duration delta = Duration.between(impl.scheduled, impl.actual).abs();
    assertTrue(delta.toMillis() < 1000);
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
    assertEquals("Cron expression contains 5 parts but we expect one of [6, 7]", e.getMessage());
  }

  @Test
  public void stepsTest() throws Exception {

    var impl = new SkedServiceImpl();
    DBOS.registerWorkflows(SkedService.class, impl);
    DBOS.launch();
    var schedulerService = DBOSTestAccess.getSchedulerService();

    Thread.sleep(5000);
    schedulerService.stop();
    Thread.sleep(1000);

    var input = new ListWorkflowsInput.Builder().workflowName("withSteps").build();
    var workflows = DBOS.listWorkflows(input);
    assertTrue(workflows.size() <= 2);

    var steps = DBOS.listWorkflowSteps(workflows.get(0).workflowId());
    assertEquals(2, steps.size());
  }
}
