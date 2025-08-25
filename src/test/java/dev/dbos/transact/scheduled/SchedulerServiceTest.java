package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.*;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchedulerServiceTest {

    private static DBOSConfig dbosConfig;
    private DBOS dbos;
    private SystemDatabase systemDatabase;
    private SchedulerService schedulerService;

    @BeforeAll
    static void onetimeSetup() throws Exception {
        SchedulerServiceTest.dbosConfig = new DBOSConfig.Builder().name("systemdbtest")
                .dbHost("localhost").dbPort(5432).dbUser("postgres").sysDbName("dbos_java_sys")
                .maximumPoolSize(2).build();
    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        DBUtils.recreateDB(dbosConfig);
        dbos = DBOS.initialize(dbosConfig);
        systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
        schedulerService = DBOSTestAccess.getSchedulerService(dbos);

        dbos.launch();
    }

    @AfterEach
    void afterEachTest() throws Exception {
        // let scheduled workflows drain
        Thread.sleep(1000);
        dbos.shutdown();
    }

    @Test
    public void simpleScheduledWorkflow() throws Exception {

        EverySecWorkflow swf = new EverySecWorkflow();
        dbos.scheduleWorkflow(swf);
        Thread.sleep(5000);
        schedulerService.stop();
        Thread.sleep(1000);

        int count = swf.wfCounter;
        System.out.println("Final count: " + count);
        assertTrue(count >= 2 && count <= 5);
    }

    @Test
    public void ThirdSecWorkflow() throws Exception {

        EveryThirdSec swf = new EveryThirdSec();
        dbos.scheduleWorkflow(swf);
        Thread.sleep(5000);
        schedulerService.stop();
        Thread.sleep(1000);

        int count = swf.wfCounter;
        System.out.println("Final count: " + count);
        assertTrue(count <= 2);
    }

    @Test
    public void MultipleWorkflowsTest() throws Exception {

        MultipleWorkflows swf = new MultipleWorkflows();
        dbos.scheduleWorkflow(swf);
        Thread.sleep(5000);
        schedulerService.stop();
        Thread.sleep(1000);

        int count = swf.wfCounter;
        System.out.println("Final count: " + count);
        assertTrue(count >= 2 && count <= 5);
        int count3 = swf.wfCounter3;
        System.out.println("Final count3: " + count3);
        assertTrue(count3 <= 2);
    }

    @Test
    public void TimedWorkflowsTest() throws Exception {

        TimedWorkflow swf = new TimedWorkflow();
        dbos.scheduleWorkflow(swf);
        Thread.sleep(5000);
        schedulerService.stop();
        Thread.sleep(1000);

        assertNotNull(swf.scheduled);
        assertNotNull(swf.actual);
        Duration delta = Duration.between(swf.scheduled, swf.actual).abs();
        assertTrue(delta.toMillis() < 1000);
    }

    @Test
    public void invalidMethod() {

        InvalidMethodWorkflow imv = new InvalidMethodWorkflow();

        try {
            dbos.scheduleWorkflow(imv);
            assertTrue(false); // fail if we get here
        } catch (IllegalArgumentException e) {
            assertEquals(
                    "Scheduled workflow must have parameters (Instant scheduledTime, Instant actualTime)",
                    e.getMessage());
        }
    }

    @Test
    public void invalidCron() {

        InvalidCronWorkflow icw = new InvalidCronWorkflow();

        try {
            dbos.scheduleWorkflow(icw);
            assertTrue(false); // fail if we get here
        } catch (IllegalArgumentException e) {

            System.out.println(e.getMessage());
            assertEquals("Cron expression contains 5 parts but we expect one of [6, 7]",
                    e.getMessage());
        }
    }

    @Test
    public void stepsTest() throws Exception {

        Steps steps = dbos.<Steps>Workflow().interfaceClass(Steps.class)
                .implementation(new StepsImpl()).build();

        WorkflowWithSteps swf = new WorkflowWithSteps(steps);
        dbos.scheduleWorkflow(swf);
        Thread.sleep(5000);
        schedulerService.stop();
        Thread.sleep(1000);

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertTrue(wfs.size() <= 2);

        List<StepInfo> wsteps = systemDatabase.listWorkflowSteps(wfs.get(0).getWorkflowId());
        assertEquals(2, wsteps.size());
    }

    // Manual test only do not enable and commit
    // @Test
    public void everyMinute() throws Exception {
        EveryMinute em = new EveryMinute();
        dbos.scheduleWorkflow(em);
        Thread.sleep(600000);
    }

    public static class InvalidMethodWorkflow {

        @Workflow
        @Scheduled(cron = "0/1 * * * * ?")
        public void scheduledWF(Instant scheduled, String actual) {
        }
    }

    public static class InvalidCronWorkflow {

        @Workflow
        @Scheduled(cron = "* * * * *")
        public void scheduledWF(Instant scheduled, Instant actual) {
        }
    }
}
