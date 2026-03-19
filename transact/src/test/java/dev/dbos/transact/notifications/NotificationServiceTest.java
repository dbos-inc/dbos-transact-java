package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface NotService {

  void sendWorkflow(String target, String topic, String msg);

  String recvWorkflow(String topic, Duration timeout);

  String recvMultiple(String topic);

  int recvCount(String topic);

  String concWorkflow(String topic);

  String disallowedRecvInStep();

  String recvOneMessage();

  String recvTwoMessages();

  void sendFromWF(String destination, String msg, String idempotencyKey);

  void sendFromStep(String destination, String msg, String idempotencyKey);
}

class NotServiceImpl implements NotService {

  final CountDownLatch recvReadyLatch = new CountDownLatch(1);
  final CountDownLatch recvTwoLatch = new CountDownLatch(1);

  @Workflow
  public void sendWorkflow(String target, String topic, String msg) {
    try {
      // Wait for recv to signal that it's ready
      recvReadyLatch.await();
      // Now proceed with sending
      DBOS.send(target, msg, topic);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for recv signal", e);
    }
    // DBOS.send(target, msg, topic);
  }

  @Workflow
  public String recvWorkflow(String topic, Duration timeout) {
    recvReadyLatch.countDown();
    String msg = (String) DBOS.recv(topic, timeout);
    return msg;
  }

  @Workflow
  public String recvMultiple(String topic) {
    recvReadyLatch.countDown();
    String msg1 = (String) DBOS.recv(topic, Duration.ofSeconds(5));
    String msg2 = (String) DBOS.recv(topic, Duration.ofSeconds(5));
    String msg3 = (String) DBOS.recv(topic, Duration.ofSeconds(5));
    return msg1 + msg2 + msg3;
  }

  @Workflow
  public int recvCount(String topic) {
    try {
      recvReadyLatch.await();
    } catch (InterruptedException e) {
    }
    String msg1 = (String) DBOS.recv(topic, Duration.ofSeconds(0));
    String msg2 = (String) DBOS.recv(topic, Duration.ofSeconds(0));
    String msg3 = (String) DBOS.recv(topic, Duration.ofSeconds(0));
    int rc = 0;
    if (msg1 != null) ++rc;
    if (msg2 != null) ++rc;
    if (msg3 != null) ++rc;
    return rc;
  }

  @Workflow
  public String concWorkflow(String topic) {
    recvReadyLatch.countDown();
    String message = (String) DBOS.recv(topic, Duration.ofSeconds(5));
    return message;
  }

  @Workflow
  public String disallowedRecvInStep() {
    DBOS.runStep(() -> DBOS.recv("a", Duration.ofSeconds(0)), "recv");
    return "Done";
  }

  @Workflow
  public String recvTwoMessages() {
    String msg1 = (String) DBOS.recv(null, Duration.ofSeconds(10));
    recvTwoLatch.countDown();
    String msg2 = (String) DBOS.recv(null, Duration.ofSeconds(2));
    return "%s-%s".formatted(msg1, msg2);
  }

  @Workflow
  public String recvOneMessage() {
    String msg1 = (String) DBOS.recv(null, Duration.ofSeconds(10));
    return msg1;
  }

  @Workflow
  public void sendFromWF(String destination, String msg, String idempotencyKey) {
    DBOS.send(destination, msg, null, idempotencyKey);
  }

  @Workflow
  public void sendFromStep(String destination, String msg, String idempotencyKey) {
    DBOS.runStep(() -> DBOS.send(destination, msg, null, idempotencyKey), "send");
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class NotificationServiceTest {

  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    NotificationServiceTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys");
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOSTestAccess.reinitialize(dbosConfig);
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
  public void recvSleep() throws Exception {

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
    assertEquals(1, wfs.size());
  }

  @Test
  public void sendSameIdempotencyKeyTest() throws Exception {
    // Sending with the same idempotency key twice delivers only one message.

    var impl = new NotServiceImpl();
    NotService notService = DBOS.registerWorkflows(NotService.class, impl);
    DBOS.launch();

    var handle = DBOS.startWorkflow(() -> notService.recvTwoMessages());

    String idempotencyKey = UUID.randomUUID().toString();

    DBOS.send(handle.workflowId(), "hello", null, idempotencyKey);
    impl.recvTwoLatch.await();
    // reusing the same idempotency key should not result in a duplicate message
    DBOS.send(handle.workflowId(), "hello again", null, idempotencyKey);

    // The second recv times out (returns null), proving only one message was delivered.
    assertEquals("hello-null", handle.getResult());
  }

  @Test
  public void sendDifferentIdempotencyKeyTest() throws Exception {
    // Different idempotency keys deliver separate messages.

    var impl = new NotServiceImpl();
    NotService notService = DBOS.registerWorkflows(NotService.class, impl);
    DBOS.launch();

    var handle = DBOS.startWorkflow(() -> notService.recvTwoMessages());

    DBOS.send(handle.workflowId(), "a", null, UUID.randomUUID().toString());
    impl.recvTwoLatch.await();
    DBOS.send(handle.workflowId(), "b", null, UUID.randomUUID().toString());

    assertEquals("a-b", handle.getResult());
  }

  @Test
  public void sendSameIdempotencyKeyFromWorkflowTest() throws Exception {
    // Send from a workflow with same idempotency key twice delivers only one message.

    var impl = new NotServiceImpl();
    NotService notService = DBOS.registerWorkflows(NotService.class, impl);
    DBOS.launch();

    var handle = DBOS.startWorkflow(() -> notService.recvTwoMessages());

    String idempotencyKey = UUID.randomUUID().toString();
    notService.sendFromWF(handle.workflowId(), "hello", idempotencyKey);
    impl.recvTwoLatch.await();
    notService.sendFromWF(handle.workflowId(), "hello again", idempotencyKey);

    // The second recv times out (returns null), proving only one message was delivered.
    assertEquals("hello-null", handle.getResult());
  }

  @Test
  public void sendFromStep() throws Exception {
    // Send from a step (without idempotency key).

    var impl = new NotServiceImpl();
    NotService notService = DBOS.registerWorkflows(NotService.class, impl);
    DBOS.launch();

    var handle = DBOS.startWorkflow(() -> notService.recvOneMessage());
    notService.sendFromStep(handle.workflowId(), "hello", null);

    assertEquals("hello", handle.getResult());
  }

  @Test
  public void recvCompletesAfterListenerReconnect() throws Exception {
    // Verify that signalAll() on reconnect wakes a waiting recv promptly, rather
    // than forcing it to sleep out its full dbPollingIntervalEventMs (10s).
    //
    // Sequence:
    //   1. Start a recv waiter with a 30s timeout (so it does not time out during the test).
    //   2. Stop the notification listener — simulates a connection drop.
    //   3. Send the message; NOTIFY fires but the listener is down and misses it.
    //   4. Restart the listener; on reconnect it calls signalAll(), waking the waiter.
    //   5. Assert the recv returns well within dbPollingIntervalEventMs (10s).
    //      Without signalAll() the waiter would sleep the full 10s before re-polling.

    var impl = new NotServiceImpl();
    NotService notService = DBOS.registerWorkflows(NotService.class, impl);
    DBOS.launch();
    // Do NOT call speedUpPollingForTest() — the 10s polling interval is the
    // baseline we are asserting against.

    String wfid = "reconnect-recv-wf";
    var handle =
        DBOS.startWorkflow(
            () -> notService.recvWorkflow("reconnect-topic", Duration.ofSeconds(30)),
            new StartWorkflowOptions(wfid));

    // recvReadyLatch fires immediately before DBOS.recv(); give the impl a
    // moment to register its condition and enter condition.await().
    impl.recvReadyLatch.await();
    Thread.sleep(300);

    // Drop the listener so the upcoming NOTIFY will be missed.
    DBOSTestAccess.stopNotificationListener();

    // Send while the listener is down.
    DBOS.send(wfid, "hello-reconnect", "reconnect-topic");

    long reconnectStart = System.currentTimeMillis();

    // Restart: reconnect triggers signalAll(), waking the waiter.
    DBOSTestAccess.startNotificationListener();

    assertEquals("hello-reconnect", handle.getResult());
    long elapsed = System.currentTimeMillis() - reconnectStart;
    assertTrue(
        elapsed < 8000,
        "recv should complete within 8s of listener reconnect via signalAll(), took "
            + elapsed
            + "ms");
  }

  @Test
  public void sendFromStepWithIdempotencyKey() throws Exception {
    // Send from a step with same idempotency key twice delivers only one message.

    var impl = new NotServiceImpl();
    NotService notService = DBOS.registerWorkflows(NotService.class, impl);
    DBOS.launch();

    var handle = DBOS.startWorkflow(() -> notService.recvTwoMessages());

    String idempotencyKey = UUID.randomUUID().toString();
    notService.sendFromStep(handle.workflowId(), "hello", idempotencyKey);
    impl.recvTwoLatch.await();
    notService.sendFromStep(handle.workflowId(), "hello again", idempotencyKey);

    assertEquals("hello-null", handle.getResult());
  }
}
