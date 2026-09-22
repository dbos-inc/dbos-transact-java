package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.DebouncerClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DebouncedRows;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.QueueName;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Workflow;
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
    dbos.registerQueue(USER_QUEUE, QueueOptions.empty());

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

  // ==================== Coalescing into a debounced workflow ====================
  //
  // A newer SDK version keeps a debounced workflow waiting DELAYED on its queue, holding its
  // debounce key as its deduplication ID. The client has to coalesce into such a row rather than
  // start a service workflow beside it.

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
        new DBOSClient.EnqueueOptions(
                Constants.DEBOUNCER_WORKFLOW_NAME,
                Constants.DEBOUNCER_CLASS_NAME,
                Constants.DBOS_INTERNAL_QUEUE)
            .withDeduplicationId("process-legacy-negative"),
        new Object[] {debouncerOpts, ctx, message});

    // The user workflow still starts, at the default priority, rather than the caller's handle
    // waiting on a workflow that never appears.
    assertEquals("result:legacy", dbosClient.retrieveWorkflow(userWorkflowId).getResult());
    assertEquals(
        0, dbosClient.getWorkflowStatus(userWorkflowId).orElseThrow().priority().intValue());
  }

  @Test
  void rejectsANegativePriority() {
    // Refused at the call: the user workflow's options are only built inside the debouncer
    // workflow, where the same value would fail durably.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            debouncer()
                .withQueue(QueueName.of(USER_QUEUE))
                .withPriority(-1)
                .debounce("prio-neg", Duration.ofMillis(500), "x"));
    assertEquals(0, serviceImpl.callCount.get());
  }
}
