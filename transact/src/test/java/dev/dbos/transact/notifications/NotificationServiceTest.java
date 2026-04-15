package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.*;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AutoClose;
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

  private final DBOS dbos;

  NotServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  final CountDownLatch recvReadyLatch = new CountDownLatch(1);
  final CountDownLatch recvTwoLatch = new CountDownLatch(1);

  @Workflow
  @Override
  public void sendWorkflow(String target, String topic, String msg) {
    try {
      // Wait for recv to signal that it's ready
      recvReadyLatch.await();
      // Now proceed with sending
      dbos.send(target, msg, topic);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for recv signal", e);
    }
  }

  @Workflow
  @Override
  public String recvWorkflow(String topic, Duration timeout) {
    recvReadyLatch.countDown();
    String msg = dbos.<String>recv(topic, timeout).orElse(null);
    return msg;
  }

  @Workflow
  @Override
  public String recvMultiple(String topic) {
    recvReadyLatch.countDown();
    String msg1 = dbos.<String>recv(topic, Duration.ofSeconds(5)).orElseThrow();
    String msg2 = dbos.<String>recv(topic, Duration.ofSeconds(5)).orElseThrow();
    String msg3 = dbos.<String>recv(topic, Duration.ofSeconds(5)).orElseThrow();
    return msg1 + msg2 + msg3;
  }

  @Workflow
  @Override
  public int recvCount(String topic) {
    try {
      recvReadyLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for recv signal", e);
    }
    String msg1 = dbos.<String>recv(topic, Duration.ofSeconds(0)).orElse(null);
    String msg2 = dbos.<String>recv(topic, Duration.ofSeconds(0)).orElse(null);
    String msg3 = dbos.<String>recv(topic, Duration.ofSeconds(0)).orElse(null);
    int rc = 0;
    if (msg1 != null) ++rc;
    if (msg2 != null) ++rc;
    if (msg3 != null) ++rc;
    return rc;
  }

  @Workflow
  @Override
  public String concWorkflow(String topic) {
    recvReadyLatch.countDown();
    String message = dbos.<String>recv(topic, Duration.ofSeconds(5)).orElseThrow();
    return message;
  }

  @Workflow
  @Override
  public String disallowedRecvInStep() {
    dbos.runStep(() -> dbos.recv("a", Duration.ofSeconds(0)).orElseThrow(), "recv");
    return "Done";
  }

  @Workflow
  @Override
  public String recvTwoMessages() {
    String msg1 = dbos.<String>recv(null, Duration.ofSeconds(10)).orElseThrow();
    recvTwoLatch.countDown();
    String msg2 = dbos.<String>recv(null, Duration.ofSeconds(2)).orElse(null);
    return "%s-%s".formatted(msg1, msg2);
  }

  @Workflow
  @Override
  public String recvOneMessage() {
    String msg1 = dbos.<String>recv(null, Duration.ofSeconds(10)).orElseThrow();
    return msg1;
  }

  @Workflow
  @Override
  public void sendFromWF(String destination, String msg, String idempotencyKey) {
    dbos.send(destination, msg, null, idempotencyKey);
  }

  @Workflow
  @Override
  public void sendFromStep(String destination, String msg, String idempotencyKey) {
    dbos.runStep(() -> dbos.send(destination, msg, null, idempotencyKey), "send");
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class NotificationServiceTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  private DBOSConfig dbosConfig;
  @AutoClose private DBOS dbos;

  @BeforeEach
  void setup() {
    this.dbosConfig = pgContainer.dbosConfig();
    this.dbos = new DBOS(dbosConfig);
  }

  @Test
  public void basic_send_recv() throws Exception {

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));

    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(
        () -> notService.recvWorkflow("topic1", Duration.ofSeconds(10)),
        new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", "HelloDBOS"),
        new StartWorkflowOptions(wfid2));

    var handle1 = dbos.retrieveWorkflow(wfid1);
    var handle2 = dbos.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertEquals("HelloDBOS", result);

    assertEquals(WorkflowState.SUCCESS, handle1.getStatus().status());
    assertEquals(WorkflowState.SUCCESS, handle2.getStatus().status());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wfid1);

    // assertEquals(1, stepInfos.size()) ; cannot do this because sleep is a maybe
    assertEquals("DBOS.recv", stepInfos.get(0).functionName());

    stepInfos = dbos.listWorkflowSteps(wfid2);
    assertEquals(1, stepInfos.size());
    assertEquals("DBOS.send", stepInfos.get(0).functionName());
  }

  @Test
  public void multiple_send_recv() throws Exception {

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
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

    assertEquals(WorkflowState.SUCCESS, handle1.getStatus().status());
  }

  @Test
  public void send_oaoo() throws Exception {
    var simpl = new NotServiceImpl(dbos);
    NotService notService = dbos.registerProxy(NotService.class, simpl);
    dbos.launch();

    String wfid1 = "recvwfc";
    var handle1 =
        dbos.startWorkflow(() -> notService.recvCount("topic1"), new StartWorkflowOptions(wfid1));

    dbos.send(wfid1, "hi", "topic1", "dothisonce");
    dbos.send(wfid1, "hi", "topic1", "dothisonce");
    dbos.send(wfid1, "hi", "topic1", "dothisonce");
    simpl.recvReadyLatch.countDown();

    assertEquals(1, handle1.getResult());
  }

  @Test
  public void notopic() throws Exception {

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(
        () -> notService.recvWorkflow(null, Duration.ofSeconds(5)),
        new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, null, "HelloDBOS"), new StartWorkflowOptions(wfid2));

    var handle1 = dbos.retrieveWorkflow(wfid1);
    var handle2 = dbos.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertEquals("HelloDBOS", result);

    assertEquals(WorkflowState.SUCCESS, handle1.getStatus().status());
    assertEquals(WorkflowState.SUCCESS, handle2.getStatus().status());
  }

  @Test
  public void noWorkflowRecv() {
    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
    dbos.launch();
    var e1 =
        assertThrows(
            IllegalStateException.class,
            () -> {
              dbos.recv("someTopic", Duration.ofSeconds(5)).orElseThrow();
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

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
    dbos.launch();

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

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(
        () -> notService.recvWorkflow("topic1", Duration.ofSeconds(5)),
        new StartWorkflowOptions(wfid1));

    String wfid2 = "sendf1";
    dbos.startWorkflow(
        () -> notService.sendWorkflow(wfid1, "topic1", null), new StartWorkflowOptions(wfid2));

    var handle1 = dbos.retrieveWorkflow(wfid1);
    var handle2 = dbos.retrieveWorkflow(wfid2);

    String result = (String) handle1.getResult();
    assertNull(result);

    assertEquals(WorkflowState.SUCCESS, handle1.getStatus().status());
    assertEquals(WorkflowState.SUCCESS, handle2.getStatus().status());
  }

  @Test
  public void timeout() {

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
    dbos.launch();

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

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
    dbos.launch();

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

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
    dbos.launch();

    String wfid1 = "recvwf1";
    dbos.startWorkflow(
        () -> notService.recvWorkflow("topic1", Duration.ofSeconds(5)),
        new StartWorkflowOptions(wfid1));

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

    assertEquals(WorkflowState.SUCCESS, handle1.getStatus().status());
    assertEquals(WorkflowState.SUCCESS, handle2.getStatus().status());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wfid1);

    assertEquals(2, stepInfos.size());
    assertEquals("DBOS.recv", stepInfos.get(0).functionName());
    assertEquals("DBOS.sleep", stepInfos.get(1).functionName());

    stepInfos = dbos.listWorkflowSteps(wfid2);
    assertEquals(1, stepInfos.size());
    assertEquals("DBOS.send", stepInfos.get(0).functionName());
  }

  @Test
  public void sendOutsideWFTest() throws Exception {

    NotService notService = dbos.registerProxy(NotService.class, new NotServiceImpl(dbos));
    dbos.launch();

    String wfid1 = "recvwf1";

    var options = new StartWorkflowOptions(wfid1);
    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(() -> notService.recvWorkflow("topic1", Duration.ofSeconds(5)), options);

    Thread.sleep(1000);

    assertEquals(WorkflowState.PENDING, handle.getStatus().status());
    dbos.send(wfid1, "hello", "topic1");

    assertEquals("hello", handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());

    List<WorkflowStatus> wfs = dbos.listWorkflows(null);
    assertEquals(1, wfs.size());
  }

  @Test
  public void sendSameIdempotencyKeyTest() throws Exception {
    // Sending with the same idempotency key twice delivers only one message.

    var impl = new NotServiceImpl(dbos);
    NotService notService = dbos.registerProxy(NotService.class, impl);
    dbos.launch();

    var handle = dbos.startWorkflow(() -> notService.recvTwoMessages());

    String idempotencyKey = UUID.randomUUID().toString();

    dbos.send(handle.workflowId(), "hello", null, idempotencyKey);
    impl.recvTwoLatch.await();
    // reusing the same idempotency key should not result in a duplicate message
    dbos.send(handle.workflowId(), "hello again", null, idempotencyKey);

    // The second recv times out (returns null), proving only one message was delivered.
    assertEquals("hello-null", handle.getResult());
  }

  @Test
  public void sendDifferentIdempotencyKeyTest() throws Exception {
    // Different idempotency keys deliver separate messages.

    var impl = new NotServiceImpl(dbos);
    NotService notService = dbos.registerProxy(NotService.class, impl);
    dbos.launch();

    var handle = dbos.startWorkflow(() -> notService.recvTwoMessages());

    dbos.send(handle.workflowId(), "a", null, UUID.randomUUID().toString());
    impl.recvTwoLatch.await();
    dbos.send(handle.workflowId(), "b", null, UUID.randomUUID().toString());

    assertEquals("a-b", handle.getResult());
  }

  @Test
  public void sendSameIdempotencyKeyFromWorkflowTest() throws Exception {
    // Send from a workflow with same idempotency key twice delivers only one message.

    var impl = new NotServiceImpl(dbos);
    NotService notService = dbos.registerProxy(NotService.class, impl);
    dbos.launch();

    var handle = dbos.startWorkflow(() -> notService.recvTwoMessages());

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

    var impl = new NotServiceImpl(dbos);
    NotService notService = dbos.registerProxy(NotService.class, impl);
    dbos.launch();

    var handle = dbos.startWorkflow(() -> notService.recvOneMessage());
    notService.sendFromStep(handle.workflowId(), "hello", null);

    assertEquals("hello", handle.getResult());
  }

  @Test
  public void sendFromStepWithIdempotencyKey() throws Exception {
    // Send from a step with same idempotency key twice delivers only one message.

    var impl = new NotServiceImpl(dbos);
    NotService notService = dbos.registerProxy(NotService.class, impl);
    dbos.launch();

    var handle = dbos.startWorkflow(() -> notService.recvTwoMessages());

    String idempotencyKey = UUID.randomUUID().toString();
    notService.sendFromStep(handle.workflowId(), "hello", idempotencyKey);
    impl.recvTwoLatch.await();
    notService.sendFromStep(handle.workflowId(), "hello again", idempotencyKey);

    assertEquals("hello-null", handle.getResult());
  }
}
