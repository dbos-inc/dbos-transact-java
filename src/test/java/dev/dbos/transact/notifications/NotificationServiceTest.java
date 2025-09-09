package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.*;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NotificationServiceTest {

    private static DBOSConfig dbosConfig;
    private DBOS dbos;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        NotificationServiceTest.dbosConfig = new DBOSConfig.Builder().name("systemdbtest")
                .dbHost("localhost").dbPort(5432).dbUser("postgres").sysDbName("dbos_java_sys")
                .maximumPoolSize(2).build();
    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        DBUtils.recreateDB(dbosConfig);
        dbos = DBOS.initialize(dbosConfig);
    }

    @AfterEach
    void afterEachTest() throws Exception {
        dbos.shutdown();
    }

    @Test
    public void basic_send_recv() throws Exception {

        // TODO was async
        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();
        var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

        String wfid1 = "recvwf1";

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid1)) {
            notService.recvWorkflow("topic1", 10);
        }

        String wfid2 = "sendf1";

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid2)) {
            notService.sendWorkflow(wfid1, "topic1", "HelloDBOS");
        }

        WorkflowHandle<?> handle1 = dbos.retrieveWorkflow(wfid1);
        WorkflowHandle<?> handle2 = dbos.retrieveWorkflow(wfid2);

        String result = (String) handle1.getResult();
        assertEquals("HelloDBOS", result);

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
        assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());

        List<StepInfo> stepInfos = systemDatabase.listWorkflowSteps(wfid1);

        // assertEquals(1, stepInfos.size()) ; cannot do this because sleep is a maybe
        assertEquals("DBOS.recv", stepInfos.get(0).getFunctionName());

        stepInfos = systemDatabase.listWorkflowSteps(wfid2);
        assertEquals(1, stepInfos.size());
        assertEquals("DBOS.send", stepInfos.get(0).getFunctionName());
    }

    @Test
    public void multiple_send_recv() throws Exception {

        // TODO was async
        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();

        String wfid1 = "recvwf1";

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid1)) {
            notService.recvMultiple("topic1");
        }

        try (var _ignore = WorkflowOptions.setWorkflowId("send1")) {
            notService.sendWorkflow(wfid1, "topic1", "Hello1");
        }
        dbos.retrieveWorkflow("send1").getResult();

        try (var _ignore = WorkflowOptions.setWorkflowId("send2")) {
            notService.sendWorkflow(wfid1, "topic1", "Hello2");
        }
        dbos.retrieveWorkflow("send2").getResult();

        try (var _ignore = WorkflowOptions.setWorkflowId("send3")) {
            notService.sendWorkflow(wfid1, "topic1", "Hello3");
        }
        dbos.retrieveWorkflow("send3").getResult();

        WorkflowHandle<?> handle1 = dbos.retrieveWorkflow(wfid1);

        String result = (String) handle1.getResult();
        assertEquals("Hello1Hello2Hello3", result);

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
    }

    @Test
    public void notopic() throws Exception {

        // TODO: was async
        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();

        String wfid1 = "recvwf1";

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid1)) {
            notService.recvWorkflow(null, 5);
        }

        String wfid2 = "sendf1";

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid2)) {
            notService.sendWorkflow(wfid1, null, "HelloDBOS");
        }

        WorkflowHandle<?> handle1 = dbos.retrieveWorkflow(wfid1);
        WorkflowHandle<?> handle2 = dbos.retrieveWorkflow(wfid2);

        String result = (String) handle1.getResult();
        assertEquals("HelloDBOS", result);

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
        assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());
    }

    @Test
    public void noWorkflowRecv() {

        dbos.launch();
        try {
            dbos.recv("someTopic", 5);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertEquals("recv() must be called from a workflow.", e.getMessage());
        }
    }

    @Test
    public void sendNotexistingID() throws Exception {

        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();

        // just to open the latch
        try (var _ignore = WorkflowOptions.setWorkflowId("abc")) {
            notService.recvWorkflow(null, 1);
        }

        try {
            try (var _ignore = WorkflowOptions.setWorkflowId("send1")) {
                notService.sendWorkflow("fakeid", "topic1", "HelloDBOS");
            }
            assertTrue(false);
        } catch (NonExistentWorkflowException e) {
            assertEquals("fakeid", e.getWorkflowId());
        }
    }

    @Test
    public void sendNull() throws Exception {

        // TODO: was async
        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();

        String wfid1 = "recvwf1";

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid1)) {
            notService.recvWorkflow("topic1", 5);
        }

        String wfid2 = "sendf1";

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid2)) {
            notService.sendWorkflow(wfid1, "topic1", null);
        }

        WorkflowHandle<?> handle1 = dbos.retrieveWorkflow(wfid1);
        WorkflowHandle<?> handle2 = dbos.retrieveWorkflow(wfid2);

        String result = (String) handle1.getResult();
        assertNull(result);

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
        assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());
    }

    @Test
    public void timeout() {

        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();

        String wfid1 = "recvwf1";

        long start = System.currentTimeMillis();
        try (var _ignore = WorkflowOptions.setWorkflowId(wfid1)) {
            notService.recvWorkflow("topic1", 3);
        }

        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 4000, "Call should return in under 4 seconds");
    }

    @Test
    public void concurrencyTest() throws Exception {

        String wfuuid = UUID.randomUUID().toString();
        String topic = "test_topic";

        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<String> future1 = executor.submit(() -> testThread(notService, wfuuid, topic));
            Future<String> future2 = executor.submit(() -> testThread(notService, wfuuid, topic));

            String expectedMessage = "test message";
            // dbos.send(wfuuid, expectedMessage, topic);
            try (var _ignore = WorkflowOptions.setWorkflowId("send1")) {
                notService.sendWorkflow(wfuuid, topic, expectedMessage);
            }

            // Both should return the same message
            String result1 = future1.get();
            String result2 = future2.get();

            assertEquals(result1, result2);
            assertEquals(expectedMessage, result1);

        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private String testThread(NotService service, String id, String topic) {
        try (var _ignore = WorkflowOptions.setWorkflowId(id)) {
            return service.concWorkflow(topic);
        }
    }

    @Test
    public void recv_sleep() throws Exception {

        // TODO: was async
        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();
        var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

        String wfid1 = "recvwf1";

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid1)) {
            notService.recvWorkflow("topic1", 5);
        }

        String wfid2 = "sendf1";

        // forcing the recv to wait on condition
        Thread.sleep(2000);

        try (var _ignore = WorkflowOptions.setWorkflowId(wfid2)) {
            notService.sendWorkflow(wfid1, "topic1", "HelloDBOS");
        }

        WorkflowHandle<?> handle1 = dbos.retrieveWorkflow(wfid1);
        WorkflowHandle<?> handle2 = dbos.retrieveWorkflow(wfid2);

        String result = (String) handle1.getResult();
        assertEquals("HelloDBOS", result);

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
        assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());

        List<StepInfo> stepInfos = systemDatabase.listWorkflowSteps(wfid1);
        // Will be 2 when we implement DBOS.sleep
        assertEquals(2, stepInfos.size());
        assertEquals("DBOS.recv", stepInfos.get(0).getFunctionName());
        assertEquals("DBOS.sleep", stepInfos.get(1).getFunctionName());

        stepInfos = systemDatabase.listWorkflowSteps(wfid2);
        assertEquals(1, stepInfos.size());
        assertEquals("DBOS.send", stepInfos.get(0).getFunctionName());
    }

    @Test
    public void sendOutsideWFTest() throws Exception {

        NotService notService = dbos.<NotService>Workflow().interfaceClass(NotService.class)
                .implementation(new NotServiceImpl()).build();

        dbos.launch();
        var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

        String wfid1 = "recvwf1";

        WorkflowOptions options = WorkflowOptions.builder().workflowId(wfid1).build();
        WorkflowHandle<String> handle = null;
        try (var _ignore = options.set()) {
            handle = dbos.startWorkflow(() -> notService.recvWorkflow("topic1", 5));
        }

        assertEquals(WorkflowState.PENDING.name(), handle.getStatus().getStatus());
        dbos.send(wfid1, "hello", "topic1");

        assertEquals("hello", handle.getResult());
        assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertEquals(2, wfs.size());

    }
}
