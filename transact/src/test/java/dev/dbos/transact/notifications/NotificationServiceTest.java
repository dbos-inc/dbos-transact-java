package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils.DBSettings;
import dev.dbos.transact.workflow.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class NotificationServiceTest {

  private static final DBSettings db = DBSettings.get();
  private DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  @BeforeEach
  void beforeEachTest() throws SQLException {
    db.recreate();

    dataSource = db.dataSource();
    dbosConfig = DBOSConfig.defaults("systemdbtest").withDataSource(dataSource);

    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  public void basic_send_recv() throws Exception {

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());

    DBOS.launch();

    String wfid1 = "recvwf1";
    DBOS.startWorkflow(
        () -> notService.recvWorkflow("topic1", Duration.ofSeconds(10)),
        new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    DBOS.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "HelloDBOS"),
        new StartWorkflowOptions(wfid2));

    var handle1 = DBOS.retrieveWorkflow(wfid1);
    var handle2 = DBOS.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertEquals("HelloDBOS", result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().status());

    List<StepInfo> stepInfos = DBOS.listWorkflowSteps(wfid1);

    // assertEquals(1, stepInfos.size()) ; cannot do this because sleep is a maybe
    assertEquals("DBOS.recv", stepInfos.get(0).functionName());

    stepInfos = DBOS.listWorkflowSteps(wfid2);
    assertEquals(1, stepInfos.size());
    assertEquals("DBOS.send", stepInfos.get(0).functionName());
  }

  @Test
  public void multiple_send_recv() throws Exception {

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();

    String wfid1 = "recvwf1";
    DBOS.startWorkflow(() -> notService.recvMultiple("topic1"), new StartWorkflowOptions(wfid1));

    DBOS.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "Hello1"),
        new StartWorkflowOptions("send1"));
    DBOS.retrieveWorkflow("send1").getResult();

    DBOS.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "Hello2"),
        new StartWorkflowOptions("send2"));
    DBOS.retrieveWorkflow("send2").getResult();

    DBOS.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "Hello3"),
        new StartWorkflowOptions("send3"));
    DBOS.retrieveWorkflow("send3").getResult();

    var handle1 = DBOS.retrieveWorkflow(wfid1);

    String result = (String) handle1.getResult();
    assertEquals("Hello1Hello2Hello3", result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
  }

  @Test
  public void send_oaoo() throws Exception {
    var simpl = new NotServiceImpl();
    NotService notService = DBOS.registerWorkflows(NotService.class, simpl);
    DBOS.launch();

    String wfid1 = "recvwfc";
    var handle1 =
        DBOS.startWorkflow(() -> notService.recvCount("topic1"), new StartWorkflowOptions(wfid1));

    DBOS.send(wfid1, "hi", "topic1", "dothisonce");
    DBOS.send(wfid1, "hi", "topic1", "dothisonce");
    DBOS.send(wfid1, "hi", "topic1", "dothisonce");
    simpl.recvReadyLatch.countDown();

    assertEquals(1, handle1.getResult());
  }

  @Test
  public void notopic() throws Exception {

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();

    String wfid1 = "recvwf1";
    DBOS.startWorkflow(
        () -> notService.recvWorkflow(null, Duration.ofSeconds(5)),
        new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    DBOS.startWorkflow(
        () -> notService.sendWorkflow(wfid1, null, "HelloDBOS"), new StartWorkflowOptions(wfid2));

    var handle1 = DBOS.retrieveWorkflow(wfid1);
    var handle2 = DBOS.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertEquals("HelloDBOS", result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().status());
  }

  @Test
  public void noWorkflowRecv() {
    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();
    var e1 =
        assertThrows(
            IllegalStateException.class,
            () -> {
              DBOS.recv("someTopic", Duration.ofSeconds(5));
            });
    assertEquals("DBOS.recv() must be called from a workflow.", e1.getMessage());
    var e2 =
        assertThrows(
            IllegalStateException.class,
            () -> {
              notService.disallowedRecvInStep();
            });
    assertEquals("DBOS.recv() must not be called from within a step.", e2.getMessage());
    var e3 =
        assertThrows(
            IllegalStateException.class,
            () -> {
              notService.disallowedSendInStep();
            });
    assertEquals("DBOS.send() must not be called from within a step.", e3.getMessage());
  }

  @Test
  public void sendNotexistingID() throws Exception {

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();

    // just to open the latch
    try (var id = new WorkflowOptions("abc").setContext()) {
      notService.recvWorkflow(null, Duration.ofSeconds(1));
    }

    try (var id = new WorkflowOptions("send1").setContext()) {
      assertThrows(
          RuntimeException.class, () -> notService.sendWorkflow("fakeid", "topic1", "HelloDBOS"));
    }
  }

  @Test
  public void sendNull() throws Exception {

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();

    String wfid1 = "recvwf1";
    DBOS.startWorkflow(
        () -> notService.recvWorkflow("topic1", Duration.ofSeconds(5)),
        new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    DBOS.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", null), new StartWorkflowOptions(wfid2));

    var handle1 = DBOS.retrieveWorkflow(wfid1);
    var handle2 = DBOS.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertNull(result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().status());
  }

  @Test
  public void timeout() {

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();

    String wfid1 = "recvwf1";

    long start = System.currentTimeMillis();
    String rv;
    try (var id = new WorkflowOptions(wfid1).setContext()) {
      rv = notService.recvWorkflow("topic1", Duration.ofSeconds(1));
    }

    assertNull(rv);

    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 3000, "Call should return in under 3 seconds");
  }

  @Test
  public void concurrencyTest() throws Exception {

    String wfuuid = UUID.randomUUID().toString();
    String topic = "test_topic";

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> future1 = executor.submit(() -> testThread(notService, wfuuid, topic));
      Future<String> future2 = executor.submit(() -> testThread(notService, wfuuid, topic));

      String expectedMessage = "test message";
      // DBOS.send(wfuuid, expectedMessage, topic);
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

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();

    String wfid1 = "recvwf1";
    DBOS.startWorkflow(
        () -> notService.recvWorkflow("topic1", Duration.ofSeconds(5)),
        new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";

    // forcing the recv to wait on condition
    Thread.sleep(2000);

    DBOS.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "HelloDBOS"),
        new StartWorkflowOptions(wfid2));

    var handle1 = DBOS.retrieveWorkflow(wfid1);
    var handle2 = DBOS.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertEquals("HelloDBOS", result);

    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().status());

    List<StepInfo> stepInfos = DBOS.listWorkflowSteps(wfid1);

    assertEquals(2, stepInfos.size());
    assertEquals("DBOS.recv", stepInfos.get(0).functionName());
    assertEquals("DBOS.sleep", stepInfos.get(1).functionName());

    stepInfos = DBOS.listWorkflowSteps(wfid2);
    assertEquals(1, stepInfos.size());
    assertEquals("DBOS.send", stepInfos.get(0).functionName());
  }

  @Test
  public void sendOutsideWFTest() throws Exception {

    NotService notService = DBOS.registerWorkflows(NotService.class, new NotServiceImpl());
    DBOS.launch();

    String wfid1 = "recvwf1";

    var options = new StartWorkflowOptions(wfid1);
    WorkflowHandle<String, ?> handle =
        DBOS.startWorkflow(() -> notService.recvWorkflow("topic1", Duration.ofSeconds(5)), options);

    Thread.sleep(1000);

    assertEquals(WorkflowState.PENDING.name(), handle.getStatus().status());
    DBOS.send(wfid1, "hello", "topic1");

    assertEquals("hello", handle.getResult());
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());

    List<WorkflowStatus> wfs = DBOS.listWorkflows(null);
    assertEquals(2, wfs.size());
  }
}
