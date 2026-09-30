package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.DebouncerClient;
import dev.dbos.transact.EnqueueOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DebouncedRows;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.QueueName;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.DebouncerContextOptions;
import dev.dbos.transact.workflow.internal.DebouncerMessage;
import dev.dbos.transact.workflow.internal.DebouncerOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

interface ClientTargetService {
  String process(String input);
}

class ClientTargetServiceImpl implements ClientTargetService {
  final AtomicInteger callCount = new AtomicInteger();
  final ConcurrentLinkedQueue<String> callArgs = new ConcurrentLinkedQueue<>();

  @Override
  @Workflow
  public String process(String input) {
    callCount.incrementAndGet();
    callArgs.add(input);
    return "result:" + input;
  }
}

public class DebouncerClientTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOSClient dbosClient;

  static final String USER_QUEUE = "client-user-queue";

  ClientTargetServiceImpl serviceImpl;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    serviceImpl = new ClientTargetServiceImpl();
    dbos.registerProxy(ClientTargetService.class, serviceImpl);
    dbos.launch();
    dbos.registerQueue(USER_QUEUE, new QueueOptions());

    dbosClient =
        new DBOSClient(pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password());
  }

  private DebouncerClient<String> debouncer() {
    return dbosClient
        .<String>debouncer("process")
        .withClassName(ClientTargetServiceImpl.class.getName());
  }

  @Test
  void singleCallFiresOnce() throws Exception {
    var handle = debouncer().debounce("key-1", Duration.ofMillis(500), "hello");
    assertEquals("result:hello", handle.getResult());
    assertEquals(1, serviceImpl.callCount.get());
  }

  @Test
  void multipleCallsCoalesceToLatestArgs() throws Exception {
    var d = debouncer();
    // Use a long period (3s) so the window cannot close between the three calls even on slow CI.
    var h1 = d.debounce("key-2", Duration.ofSeconds(3), "v1");
    Thread.sleep(100);
    var h2 = d.debounce("key-2", Duration.ofSeconds(3), "v2");
    Thread.sleep(100);
    var h3 = d.debounce("key-2", Duration.ofSeconds(3), "v3");

    String result = h3.getResult();
    assertEquals("result:v3", result);
    assertEquals(h1.workflowId(), h2.workflowId());
    assertEquals(h2.workflowId(), h3.workflowId());
    assertEquals(1, serviceImpl.callCount.get());
  }

  @Test
  void differentKeysFireIndependently() throws Exception {
    var d = debouncer();
    var hA = d.debounce("key-A", Duration.ofMillis(400), "A");
    var hB = d.debounce("key-B", Duration.ofMillis(400), "B");

    assertNotEquals(hA.workflowId(), hB.workflowId());
    assertEquals("result:A", hA.getResult());
    assertEquals("result:B", hB.getResult());
    assertEquals(2, serviceImpl.callCount.get());
  }

  @Test
  void reDebounceAfterWindowCloses() throws Exception {
    var d = debouncer();

    var h1 = d.debounce("key-r", Duration.ofMillis(300), "first");
    assertEquals("result:first", h1.getResult());
    assertEquals(1, serviceImpl.callCount.get());

    Thread.sleep(200);

    var h2 = d.debounce("key-r", Duration.ofMillis(300), "second");
    assertEquals("result:second", h2.getResult());
    assertEquals(2, serviceImpl.callCount.get());

    assertNotEquals(h1.workflowId(), h2.workflowId());
  }

  @Test
  void debouncerClientWithQueue() throws Exception {
    var handle =
        debouncer()
            .withQueue(QueueName.of(USER_QUEUE))
            .debounce("key-q", Duration.ofMillis(400), "queued");

    assertEquals("result:queued", handle.getResult());

    var status = dbosClient.getWorkflowStatus(handle.workflowId()).orElseThrow();
    assertEquals(WorkflowState.SUCCESS, status.status());
    assertEquals(USER_QUEUE, status.queueName());
    assertEquals(1, serviceImpl.callCount.get());
  }

  @Test
  void debouncerClientForwardsAttributes() throws Exception {
    var attributes = Map.<String, Object>of("source", "debouncer-client");
    var handle =
        debouncer().withAttributes(attributes).debounce("key-attr", Duration.ofMillis(400), "attr");

    assertEquals("result:attr", handle.getResult());

    var status = dbosClient.getWorkflowStatus(handle.workflowId()).orElseThrow();
    assertEquals(attributes, status.attributes());
  }

  // ==================== The debounced workflow's row ====================

  @Test
  void writesTheDebouncedWorkflowDelayedOnTheInternalQueue() throws Exception {
    long before = System.currentTimeMillis();
    var handle =
        debouncer().withTimeout(Duration.ofMinutes(2)).debounce("row", Duration.ofSeconds(30), "a");

    var row = DebouncedRows.read(dataSource, handle.workflowId());
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    assertEquals(Constants.DBOS_INTERNAL_QUEUE, row.queueName());
    assertEquals("process-row", row.deduplicationId());
    assertTrue(row.isDebounced());
    assertTrue(row.delayUntilEpochMs() >= before + 30_000);
    assertEquals(
        Duration.ofMinutes(2).toMillis(),
        dbosClient.getWorkflowStatus(handle.workflowId()).orElseThrow().timeoutMs());
    assertEquals(0, DebouncedRows.countByName(dataSource, Constants.DEBOUNCER_WORKFLOW_NAME));

    var again = debouncer().debounce("row", Duration.ofSeconds(30), "b");
    assertEquals(handle.workflowId(), again.workflowId());
  }

  @Test
  void writesTheDebouncedWorkflowOnTheUserQueueWithItsPriority() throws Exception {
    var handle =
        debouncer()
            .withQueue(QueueName.of(USER_QUEUE))
            .withPriority(7)
            .debounce("row", Duration.ofSeconds(30), "a");

    var row = DebouncedRows.read(dataSource, handle.workflowId());
    assertEquals(USER_QUEUE, row.queueName());
    assertEquals("process-row", row.deduplicationId());
    assertTrue(row.isDebounced());
    assertEquals(7, row.priority());
  }

  @Test
  void capsTheDelayAtTheDebounceDeadline() throws Exception {
    var d = debouncer().withDebounceTimeout(Duration.ofSeconds(20));
    var handle = d.debounce("cap", Duration.ofMinutes(5), "a");

    var row = DebouncedRows.read(dataSource, handle.workflowId());
    assertNotNull(row.debounceDeadlineEpochMs());
    assertEquals(row.debounceDeadlineEpochMs(), row.delayUntilEpochMs());
    d.debounce("cap", Duration.ofMinutes(5), "b");
    assertEquals(
        row.debounceDeadlineEpochMs(),
        DebouncedRows.read(dataSource, handle.workflowId()).delayUntilEpochMs());
  }

  @Test
  @SuppressWarnings("removal")
  void ignoresTheDeduplicationId() throws Exception {
    var handle =
        debouncer()
            .withQueue(QueueName.of(USER_QUEUE))
            .withDeduplicationId("user-dedup")
            .debounce("dd", Duration.ofSeconds(1), "a");

    assertEquals(
        "process-dd", DebouncedRows.read(dataSource, handle.workflowId()).deduplicationId());
    assertEquals("result:a", handle.getResult());
  }

  // ==================== Debouncer workflows of SDK versions before 1.2 ====================

  private String plantDebouncerWorkflow(
      String key, String promisedId, String queue, String appVersion) throws Exception {
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var options =
        new DebouncerOptions(
            "process",
            ClientTargetServiceImpl.class.getName(),
            null,
            queue,
            null,
            null,
            null,
            null);
    var ctx = new DebouncerContextOptions(promisedId, null, null);
    var initial =
        new DebouncerMessage(
            UUID.randomUUID().toString(), new Object[] {"stale"}, Duration.ofSeconds(2));
    var inputs =
        SerializationUtil.serializeArgs(new Object[] {options, ctx, initial}, null, null, null);
    return DebouncedRows.insertDebouncerWorkflow(
        dataSource,
        "process-" + key,
        WorkflowState.ENQUEUED,
        inputs.serializedValue(),
        inputs.serialization(),
        appVersion,
        executor.appName());
  }

  @Test
  void forwardsToALiveDebouncerWorkflow() throws Exception {
    plantDebouncerWorkflow(
        "svc", "promised-c1", null, DBOSTestAccess.getDbosExecutor(dbos).appVersion());

    var handle = debouncer().debounce("svc", Duration.ofMillis(300), "fresh");

    assertEquals("promised-c1", handle.workflowId());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), List.copyOf(serviceImpl.callArgs));
  }

  @Test
  void forwardsToALiveDebouncerWorkflowForAUserQueue() throws Exception {
    plantDebouncerWorkflow(
        "svc", "promised-c2", USER_QUEUE, DBOSTestAccess.getDbosExecutor(dbos).appVersion());

    var handle =
        debouncer()
            .withQueue(QueueName.of(USER_QUEUE))
            .debounce("svc", Duration.ofMillis(300), "fresh");

    assertEquals("promised-c2", handle.workflowId());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(1, serviceImpl.callCount.get());
  }

  @Test
  void takesOverAStrandedDebouncerWorkflowUnderItsPromisedId() throws Exception {
    var stranded = plantDebouncerWorkflow("stranded", "promised-c3", null, "no-such-version");

    var handle = debouncer().debounce("stranded", Duration.ofMillis(300), "fresh");

    assertEquals("promised-c3", handle.workflowId());
    assertEquals(
        WorkflowState.CANCELLED, dbosClient.getWorkflowStatus(stranded).orElseThrow().status());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), List.copyOf(serviceImpl.callArgs));
  }

  private void enqueuePromised(String promisedId) {
    dbosClient.enqueueWorkflow(
        new EnqueueOptions(
                "process", ClientTargetServiceImpl.class.getName(), QueueName.of(USER_QUEUE))
            .withWorkflowId(promisedId),
        new Object[] {"from-debouncer"});
  }

  @Test
  void cancelsAStrandedDebouncerWorkflowThatAlreadyStartedItsWorkflow() throws Exception {
    // Its node started the promised workflow, then died before the debouncer workflow finished.
    var stranded = plantDebouncerWorkflow("started", "promised-c4", null, "no-such-version");
    enqueuePromised("promised-c4");
    assertEquals("result:from-debouncer", dbosClient.retrieveWorkflow("promised-c4").getResult());

    var handle = debouncer().debounce("started", Duration.ofMillis(300), "fresh");

    assertEquals(
        WorkflowState.CANCELLED, dbosClient.getWorkflowStatus(stranded).orElseThrow().status());
    assertNotEquals("promised-c4", handle.workflowId());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(2, serviceImpl.callCount.get());
  }

  @Test
  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_DEBOUNCE_TAKEOVER) // one global trigger slot
  void startsOverWhenASlowDebouncerWorkflowStartsThePromisedWorkflowFirst() throws Exception {
    var slow = plantDebouncerWorkflow("slow", "promised-c5", null, "no-such-version");

    // A node of the old version was alive after all: between the cancel and the create, its
    // debouncer workflow starts the promised workflow with the arguments it had.
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_DEBOUNCE_TAKEOVER,
        new DebugTriggers.DebugAction().setCallback(() -> enqueuePromised("promised-c5")));
    WorkflowHandle<String, ?> handle;
    try {
      handle = debouncer().debounce("slow", Duration.ofMillis(300), "fresh");
    } finally {
      DebugTriggers.clearDebugTriggers();
    }

    assertNotEquals("promised-c5", handle.workflowId());
    assertEquals("result:fresh", handle.getResult());
    assertEquals("result:from-debouncer", dbosClient.retrieveWorkflow("promised-c5").getResult());
    assertEquals(
        WorkflowState.CANCELLED, dbosClient.getWorkflowStatus(slow).orElseThrow().status());
  }

  // ==================== Coalescing into a debounced workflow ====================

  private DebouncedRows.Spec debouncedRow(String queue, String workflowName, long delayUntil) {
    return debouncedRow(queue, workflowName, delayUntil, null);
  }

  private DebouncedRows.Spec debouncedRow(
      String queue, String workflowName, long delayUntil, String serializationFormat) {
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var stale =
        SerializationUtil.serializeArgs(new Object[] {"stale"}, null, serializationFormat, null);
    return new DebouncedRows.Spec(
        workflowName,
        ClientTargetServiceImpl.class.getName(),
        null,
        queue,
        "process-mixed",
        delayUntil,
        null,
        stale.serializedValue(),
        stale.serialization(),
        executor.appVersion(),
        executor.appName());
  }

  @Test
  void coalescesIntoADebouncedWorkflowWaitingOnTheInternalQueue() throws Exception {
    long planted = System.currentTimeMillis() + 60_000;
    var waiting =
        DebouncedRows.insert(
            dataSource, debouncedRow(Constants.DBOS_INTERNAL_QUEUE, "process", planted));

    var handle = debouncer().debounce("mixed", Duration.ofMillis(500), "fresh");

    assertEquals(waiting, handle.workflowId());
    assertTrue(DebouncedRows.read(dataSource, waiting).delayUntilEpochMs() < planted);
    assertEquals(0, DebouncedRows.countByName(dataSource, Constants.DEBOUNCER_WORKFLOW_NAME));
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), List.copyOf(serviceImpl.callArgs));
  }

  @Test
  void coalescesIntoADebouncedWorkflowWaitingOnAUserQueue() throws Exception {
    long planted = System.currentTimeMillis() + 60_000;
    var waiting = DebouncedRows.insert(dataSource, debouncedRow(USER_QUEUE, "process", planted));

    var handle =
        debouncer()
            .withQueue(QueueName.of(USER_QUEUE))
            .debounce("mixed", Duration.ofMillis(500), "fresh");

    assertEquals(waiting, handle.workflowId());
    assertTrue(DebouncedRows.read(dataSource, waiting).delayUntilEpochMs() < planted);
    assertEquals(0, DebouncedRows.countByName(dataSource, Constants.DEBOUNCER_WORKFLOW_NAME));
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), List.copyOf(serviceImpl.callArgs));
  }

  @Test
  void bouncesAPortableRowInItsOwnFormat() throws Exception {
    // A workflow whose row holds portable arguments -- one another language enqueued, or a
    // portable workflow here. withSerialization is what lets a bounce replace those arguments in
    // the format the row is read back in; the default would write java_jackson over them.
    long planted = System.currentTimeMillis() + 60_000;
    var waiting =
        DebouncedRows.insert(
            dataSource, debouncedRow(USER_QUEUE, "process", planted, SerializationUtil.PORTABLE));

    var handle =
        debouncer()
            .withQueue(QueueName.of(USER_QUEUE))
            .withSerialization(SerializationStrategy.PORTABLE)
            .debounce("mixed", Duration.ofMillis(500), "fresh");

    assertEquals(waiting, handle.workflowId());
    var row = DebouncedRows.read(dataSource, waiting);
    assertEquals(SerializationUtil.PORTABLE, row.serialization());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), List.copyOf(serviceImpl.callArgs));
  }

  @Test
  void refusesAKeyHeldByADifferentDebouncedWorkflow() throws Exception {
    long planted = System.currentTimeMillis() + 60_000;
    var other =
        DebouncedRows.insert(
            dataSource, debouncedRow(Constants.DBOS_INTERNAL_QUEUE, "other", planted));

    assertThrows(
        DBOSQueueDuplicatedException.class,
        () -> debouncer().debounce("mixed", Duration.ofMillis(500), "fresh"));

    assertEquals(planted, DebouncedRows.read(dataSource, other).delayUntilEpochMs());
    assertEquals(0, serviceImpl.callCount.get());
  }

  @Test
  void rejectsAPriorityWithoutAQueue() {
    var e =
        assertThrows(
            IllegalArgumentException.class,
            () -> debouncer().withPriority(3).debounce("prio", Duration.ofMillis(500), "x"));

    assertTrue(e.getMessage().contains("queue"), e.getMessage());
    assertEquals(0, serviceImpl.callCount.get());
  }

  @Test
  void aLegacyNegativePriorityIsClampedOnReplay() throws Exception {
    // Before 1.1 debounce() accepted a negative priority, so a debouncer workflow enqueued then can
    // carry one into recovery. Build that workflow directly, as debounce() now refuses to.
    var userWorkflowId = UUID.randomUUID().toString();
    var debouncerOpts =
        new DebouncerOptions(
            "process",
            ClientTargetServiceImpl.class.getName(),
            null,
            USER_QUEUE,
            null,
            null,
            -5,
            null);
    var ctx = new DebouncerContextOptions(userWorkflowId, null, null);
    var message =
        new DebouncerMessage(
            UUID.randomUUID().toString(), new Object[] {"legacy"}, Duration.ofMillis(200));
    dbosClient.enqueueWorkflow(
        new EnqueueOptions(
                Constants.DEBOUNCER_WORKFLOW_NAME,
                Constants.DEBOUNCER_CLASS_NAME,
                QueueName.of(Constants.DBOS_INTERNAL_QUEUE))
            .withDeduplicationId("process-legacy-negative"),
        new Object[] {debouncerOpts, ctx, message});

    // The user workflow still starts, at the default priority, rather than the caller's handle
    // waiting on a workflow that never appears.
    assertEquals("result:legacy", dbosClient.retrieveWorkflow(userWorkflowId).getResult());
    assertEquals(
        0, dbosClient.getWorkflowStatus(userWorkflowId).orElseThrow().priority().intValue());
  }

  @Test
  void rejectsANegativePriorityWhenSet() {
    var debouncer = debouncer().withQueue(QueueName.of(USER_QUEUE));

    assertThrows(IllegalArgumentException.class, () -> debouncer.withPriority(-1));
    // Zero is the default priority, and null clears one.
    debouncer.withPriority(0);
    debouncer.withPriority(null);
  }
}
