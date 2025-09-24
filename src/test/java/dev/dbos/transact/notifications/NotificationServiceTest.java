package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
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
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
class NotificationServiceTest {

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    NotificationServiceTest.dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .dbHost("localhost")
            .dbPort(5432)
            .dbUser("postgres")
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .build();
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

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(
        () -> notService.recvWorkflow("topic1", 10), new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "HelloDBOS"),
        new StartWorkflowOptions(wfid2));

    var handle1 = dbos.retrieveWorkflow(wfid1);
    var handle2 = dbos.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertEquals("HelloDBOS", result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wfid1);

    // assertEquals(1, stepInfos.size()) ; cannot do this because sleep is a maybe
    assertEquals("DBOS.recv", stepInfos.get(0).getFunctionName());

    stepInfos = dbos.listWorkflowSteps(wfid2);
    assertEquals(1, stepInfos.size());
    assertEquals("DBOS.send", stepInfos.get(0).getFunctionName());
  }

  @Test
  public void multiple_send_recv() throws Exception {

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(() -> notService.recvMultiple("topic1"), new StartWorkflowOptions(wfid1));

    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "Hello1"),
        new StartWorkflowOptions("send1"));
    dbos.retrieveWorkflow("send1").getResult();

    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "Hello2"),
        new StartWorkflowOptions("send2"));
    dbos.retrieveWorkflow("send2").getResult();

    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "Hello3"),
        new StartWorkflowOptions("send3"));
    dbos.retrieveWorkflow("send3").getResult();

    var handle1 = dbos.retrieveWorkflow(wfid1);

    String result = (String) handle1.getResult();
    assertEquals("Hello1Hello2Hello3", result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
  }

  @Test
  public void notopic() throws Exception {

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(() -> notService.recvWorkflow(null, 5), new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, null, "HelloDBOS"), new StartWorkflowOptions(wfid2));

    var handle1 = dbos.retrieveWorkflow(wfid1);
    var handle2 = dbos.retrieveWorkflow(wfid2);

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

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    // just to open the latch
    try (var id = new WorkflowOptions("abc").setContext()) {
      notService.recvWorkflow(null, 1);
    }

    try (var id = new WorkflowOptions("send1").setContext()) {
      assertThrows(
          RuntimeException.class, () -> notService.sendWorkflow("fakeid", "topic1", "HelloDBOS"));
    }
  }

  @Test
  public void sendNull() throws Exception {

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(() -> notService.recvWorkflow("topic1", 5), new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", null), new StartWorkflowOptions(wfid2));

    var handle1 = dbos.retrieveWorkflow(wfid1);
    var handle2 = dbos.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertNull(result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());
  }

  @Test
  public void timeout() {

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    String wfid1 = "recvwf1";

    long start = System.currentTimeMillis();
    try (var id = new WorkflowOptions(wfid1).setContext()) {
      notService.recvWorkflow("topic1", 3);
    }

    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 4000, "Call should return in under 4 seconds");
  }

  @Test
  public void concurrencyTest() throws Exception {

    String wfuuid = UUID.randomUUID().toString();
    String topic = "test_topic";

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> future1 = executor.submit(() -> testThread(notService, wfuuid, topic));
      Future<String> future2 = executor.submit(() -> testThread(notService, wfuuid, topic));

      String expectedMessage = "test message";
      // dbos.send(wfuuid, expectedMessage, topic);
      try (var id = new WorkflowOptions("send1").setContext()) {
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
    try (var context = new WorkflowOptions(id).setContext()) {
      return service.concWorkflow(topic);
    }
  }

  @Test
  public void recv_sleep() throws Exception {

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(() -> notService.recvWorkflow("topic1", 5), new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";

    // forcing the recv to wait on condition
    Thread.sleep(2000);

    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "HelloDBOS"),
        new StartWorkflowOptions(wfid2));

    var handle1 = dbos.retrieveWorkflow(wfid1);
    var handle2 = dbos.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertEquals("HelloDBOS", result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wfid1);
    // Will be 2 when we implement DBOS.sleep
    assertEquals(2, stepInfos.size());
    assertEquals("DBOS.recv", stepInfos.get(0).getFunctionName());
    assertEquals("DBOS.sleep", stepInfos.get(1).getFunctionName());

    stepInfos = dbos.listWorkflowSteps(wfid2);
    assertEquals(1, stepInfos.size());
    assertEquals("DBOS.send", stepInfos.get(0).getFunctionName());
  }

  @Test
  public void sendOutsideWFTest() throws Exception {

    NotService notService =
        dbos.<NotService>Workflow()
            .interfaceClass(NotService.class)
            .implementation(new NotServiceImpl())
            .build();

    dbos.launch();

    String wfid1 = "recvwf1";

    var options = new StartWorkflowOptions(wfid1);
    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(() -> notService.recvWorkflow("topic1", 5), options);

    Thread.sleep(1000);

    assertEquals(WorkflowState.PENDING.name(), handle.getStatus().getStatus());
    dbos.send(wfid1, "hello", "topic1");

    assertEquals("hello", handle.getResult());
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, wfs.size());
  }
}
