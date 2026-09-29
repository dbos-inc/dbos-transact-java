package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DebouncedRows;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.internal.DebouncerContextOptions;
import dev.dbos.transact.workflow.internal.DebouncerMessage;
import dev.dbos.transact.workflow.internal.DebouncerOptions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class DebouncerTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  // Per-instance counters so parallel test methods do not interfere.
  public interface DebouncedService {
    String process(String input);

    int callCount();

    java.util.List<String> callArgs();
  }

  public static class DebouncedServiceImpl implements DebouncedService {
    private final AtomicInteger callCount = new AtomicInteger();
    private final ConcurrentLinkedQueue<String> callArgs = new ConcurrentLinkedQueue<>();
    // When set, the workflow blocks here while running so tests can inspect its in-flight status.
    volatile CountDownLatch gate;

    @Override
    @Workflow
    public String process(String input) {
      callCount.incrementAndGet();
      callArgs.add(input);
      if (gate != null) {
        try {
          // Ceiling only; the test counts the gate down as soon as it has observed the status.
          // Must exceed the observation window so the workflow stays in-flight until then.
          gate.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return "result:" + input;
    }

    @Override
    public int callCount() {
      return callCount.get();
    }

    @Override
    public java.util.List<String> callArgs() {
      return java.util.List.copyOf(callArgs);
    }
  }

  DebouncedServiceImpl serviceImpl;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    serviceImpl = new DebouncedServiceImpl();
  }

  @Test
  public void negativePriorityIsRejectedAtTheCall() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    // The user workflow's options are only built inside the debouncer workflow, where the same
    // value would fail durably; the debouncer refuses it up front instead.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.<String>debouncer()
                .withQueue("any-queue")
                .withPriority(-1)
                .debounce("user-neg", Duration.ofSeconds(1), () -> svc.process("v1")));
    assertEquals(0, serviceImpl.callCount());
  }

  @Test
  public void singleCallFiresOnce() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var handle =
        dbos.<String>debouncer().debounce("user-1", Duration.ofSeconds(1), () -> svc.process("v1"));
    String result = handle.getResult();
    assertEquals("result:v1", result);
    assertEquals(1, serviceImpl.callCount());
    assertEquals(List.of("v1"), serviceImpl.callArgs());
  }

  @Test
  public void multipleCallsCoalesceToLatestArgs() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var debouncer = dbos.<String>debouncer();
    var h1 = debouncer.debounce("user-2", Duration.ofMillis(800), () -> svc.process("v1"));
    Thread.sleep(200);
    var h2 = debouncer.debounce("user-2", Duration.ofMillis(800), () -> svc.process("v2"));
    Thread.sleep(200);
    var h3 = debouncer.debounce("user-2", Duration.ofMillis(800), () -> svc.process("v3"));

    String result = h3.getResult();
    assertEquals("result:v3", result);
    // The three handles all point to the same final user workflow.
    assertEquals(h1.workflowId(), h2.workflowId());
    assertEquals(h2.workflowId(), h3.workflowId());
    assertEquals(1, serviceImpl.callCount());
    assertEquals(List.of("v3"), serviceImpl.callArgs());
  }

  @Test
  public void absoluteTimeoutFiresEvenIfCallsKeepArriving() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var debouncer = dbos.<String>debouncer().withDebounceTimeout(Duration.ofMillis(1500));

    var first = debouncer.debounce("user-3", Duration.ofMillis(800), () -> svc.process("v1"));
    String firstId = first.workflowId();

    // Keep extending the period — the absolute timeout should still kick in.
    long deadline = System.currentTimeMillis() + 3000;
    while (System.currentTimeMillis() < deadline && serviceImpl.callCount() == 0) {
      debouncer.debounce("user-3", Duration.ofMillis(800), () -> svc.process("vN"));
      Thread.sleep(150);
    }

    String result = first.getResult();
    assertTrue(result.startsWith("result:"));
    assertEquals(1, serviceImpl.callCount());
    assertEquals(firstId, first.workflowId());
  }

  @Test
  public void differentKeysFireIndependently() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var debouncer = dbos.<String>debouncer();
    var hA = debouncer.debounce("key-A", Duration.ofMillis(500), () -> svc.process("A"));
    var hB = debouncer.debounce("key-B", Duration.ofMillis(500), () -> svc.process("B"));

    assertNotEquals(hA.workflowId(), hB.workflowId());
    assertEquals("result:A", hA.getResult());
    assertEquals("result:B", hB.getResult());
    assertEquals(2, serviceImpl.callCount());
  }

  @Test
  public void concurrentCallsCoalesceSafely() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var debouncer = dbos.<String>debouncer();
    int n = 8;
    var pool = Executors.newFixedThreadPool(n);
    try {
      var ready = new CountDownLatch(n);
      var go = new CountDownLatch(1);
      var results = new ConcurrentLinkedQueue<String>();
      for (int i = 0; i < n; i++) {
        final String arg = "v" + i;
        pool.submit(
            () -> {
              ready.countDown();
              go.await();
              var h =
                  debouncer.debounce("user-conc", Duration.ofMillis(600), () -> svc.process(arg));
              results.add(h.workflowId());
              return null;
            });
      }
      ready.await(5, TimeUnit.SECONDS);
      go.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

      // All concurrent callers must resolve to the same future user workflow id.
      String first = results.peek();
      assertTrue(results.stream().allMatch(first::equals), "All handles must share workflow id");

      // Wait for the user workflow to complete.
      dbos.retrieveWorkflow(first).getResult();
      // Exactly one user workflow executed.
      assertEquals(1, serviceImpl.callCount());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void debouncerOnQueueRunsViaThatQueue() throws Exception {
    String userQueue = "debouncer-user-queue";
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    dbos.registerQueue(userQueue, new QueueOptions());

    var debouncer = dbos.<String>debouncer().withQueue(QueueName.of(userQueue));
    var handle = debouncer.debounce("user-q", Duration.ofMillis(500), () -> svc.process("queued"));
    assertEquals("result:queued", handle.getResult());

    var status = dbos.getWorkflowStatus(handle.workflowId()).orElseThrow();
    assertEquals(userQueue, status.queueName());
    assertEquals(1, serviceImpl.callCount());
  }

  // Workflow with numeric parameters to verify type coercion through send/recv round-trip.
  public interface NumericService {
    long compute(long value, double factor);
  }

  public static class NumericServiceImpl implements NumericService {
    @Override
    @Workflow
    public long compute(long value, double factor) {
      return (long) (value * factor);
    }
  }

  @Test
  public void numericArgsRoundTripCorrectly() throws Exception {
    NumericService svc = dbos.registerProxy(NumericService.class, new NumericServiceImpl());
    dbos.launch();

    var debouncer = dbos.<Long>debouncer();
    // First call
    var h1 = debouncer.debounce("num-key", Duration.ofMillis(600), () -> svc.compute(10L, 2.5));
    Thread.sleep(100);
    // Second call overrides args — after period the workflow runs with these values
    var h2 = debouncer.debounce("num-key", Duration.ofMillis(600), () -> svc.compute(7L, 3.0));

    assertEquals(h1.workflowId(), h2.workflowId());
    Long result = h2.getResult();
    // 7 * 3.0 = 21
    assertEquals(21L, result);
  }

  // Verify that debounce works for workflows with no return value.
  public interface VoidService {
    void doWork(String marker);
  }

  public static class VoidServiceImpl implements VoidService {
    final AtomicInteger callCount = new AtomicInteger();
    final ConcurrentLinkedQueue<String> markers = new ConcurrentLinkedQueue<>();

    @Override
    @Workflow
    public void doWork(String marker) {
      callCount.incrementAndGet();
      markers.add(marker);
    }
  }

  @Test
  public void debounceCoalescesCorrectly() throws Exception {
    var impl = new VoidServiceImpl();
    VoidService svc = dbos.registerProxy(VoidService.class, impl);
    dbos.launch();

    var debouncer = dbos.<Void>debouncer();
    var h1 = debouncer.debounce("void-key", Duration.ofMillis(500), () -> svc.doWork("a"));
    Thread.sleep(100);
    var h2 = debouncer.debounce("void-key", Duration.ofMillis(500), () -> svc.doWork("b"));

    h2.getResult();
    assertEquals(h1.workflowId(), h2.workflowId());
    assertEquals(1, impl.callCount.get());
    assertEquals(List.of("b"), List.copyOf(impl.markers));
  }

  // Verify that absoluteTimeout fires with the LATEST args, not the first.
  @Test
  public void absoluteTimeoutUsesLatestArgs() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    // Long period (5s) so normal expiry cannot fire; only the 1.5s absolute timeout can.
    var debouncer = dbos.<String>debouncer().withDebounceTimeout(Duration.ofMillis(1500));

    var h = debouncer.debounce("abs-key", Duration.ofSeconds(5), () -> svc.process("first"));
    Thread.sleep(500);
    debouncer.debounce("abs-key", Duration.ofSeconds(5), () -> svc.process("last"));

    String result = h.getResult();
    assertEquals("result:last", result);
    assertEquals(1, serviceImpl.callCount());
    assertEquals(List.of("last"), serviceImpl.callArgs());
  }

  public interface OrchestratorService {
    String debounceWithPriority(String arg);
  }

  public static class OrchestratorServiceImpl implements OrchestratorService {
    private final DBOS dbos;
    private final DebouncedService svc;
    private final String userQueue;

    public OrchestratorServiceImpl(DBOS dbos, DebouncedService svc, String userQueue) {
      this.dbos = dbos;
      this.svc = svc;
      this.userQueue = userQueue;
    }

    @Override
    @Workflow
    public String debounceWithPriority(String arg) {
      return dbos.<String>debouncer()
          .withQueue(userQueue)
          .withPriority(42)
          .debounce("prio-inner", Duration.ofMillis(400), () -> svc.process(arg))
          .getResult();
    }
  }

  // Verify that explicit withPriority() on Debouncer is forwarded to the user workflow.
  @Test
  public void explicitPriorityForwardedToUserWorkflow() throws Exception {
    String q = "prio-queue";
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch =
        dbos.registerProxy(OrchestratorService.class, new OrchestratorServiceImpl(dbos, svc, q));
    dbos.launch();
    dbos.registerQueue(q, new QueueOptions());

    var h = dbos.startWorkflow(() -> orch.debounceWithPriority("prio-val"));
    assertEquals("result:prio-val", h.getResult());

    var userWfStatus =
        dbos
            .listWorkflows(new ListWorkflowsInput().withQueueName(q).withWorkflowName("process"))
            .stream()
            .findFirst()
            .orElse(null);
    assertNotNull(userWfStatus, "user workflow 'process' not found on queue " + q);
    assertEquals(Integer.valueOf(42), userWfStatus.priority());
  }

  public interface TimedOrchestrator {
    String debounceWithInheritedTimeout(String arg);

    String debounceWithOwnTimeout(String arg);
  }

  public static class TimedOrchestratorImpl implements TimedOrchestrator {
    private final DBOS dbos;
    private final DebouncedService svc;

    public TimedOrchestratorImpl(DBOS dbos, DebouncedService svc) {
      this.dbos = dbos;
      this.svc = svc;
    }

    // Both return the debounced workflow's ID, once it has finished.

    @Override
    @Workflow
    public String debounceWithInheritedTimeout(String arg) {
      var h =
          dbos.<String>debouncer()
              .debounce("timed-inherited", Duration.ofMillis(200), () -> svc.process(arg));
      h.getResult();
      return h.workflowId();
    }

    @Override
    @Workflow
    public String debounceWithOwnTimeout(String arg) {
      try (var o = new WorkflowOptions().withTimeout(Duration.ofMinutes(2)).setContext()) {
        var h =
            dbos.<String>debouncer()
                .debounce("timed-own", Duration.ofMillis(200), () -> svc.process(arg));
        h.getResult();
        return h.workflowId();
      }
    }
  }

  /**
   * A debounce inside a timed workflow hands on neither that workflow's timeout nor its deadline.
   * The debounced workflow may start long after the call, and the parent's timeout is its own
   * budget, not the debounced workflow's (#561). A timeout the caller sets for the call still
   * reaches the debounced workflow, timed from its dequeue.
   */
  @Test
  public void debounceInATimedWorkflowPassesOnOnlyTheTimeoutSetForTheCall() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch = dbos.registerProxy(TimedOrchestrator.class, new TimedOrchestratorImpl(dbos, svc));
    dbos.launch();

    String inheritedId;
    try (var o =
        new WorkflowOptions("timed-parent-1").withTimeout(Duration.ofMinutes(5)).setContext()) {
      inheritedId = orch.debounceWithInheritedTimeout("a");
    }
    String ownId;
    try (var o =
        new WorkflowOptions("timed-parent-2").withTimeout(Duration.ofMinutes(5)).setContext()) {
      ownId = orch.debounceWithOwnTimeout("b");
    }

    assertEquals(
        0, DebouncedRows.countByName(pgContainer.dataSource(), Constants.DEBOUNCER_WORKFLOW_NAME));

    var inherited = dbos.retrieveWorkflow(inheritedId).getStatus();
    assertNull(inherited.timeoutMs());
    assertNull(inherited.deadlineEpochMs());

    var own = dbos.retrieveWorkflow(ownId).getStatus();
    assertEquals(Duration.ofMinutes(2).toMillis(), own.timeoutMs());
    assertNotNull(own.deadlineEpochMs());
  }

  /**
   * Outside a workflow, an ambient deadline around a debounce is ignored: the debounced workflow
   * may start long after it. An ambient timeout goes to the debounced workflow, timed from its
   * dequeue.
   */
  @Test
  @SuppressWarnings("removal") // exercises the deprecated deadline option
  public void debounceOutsideAWorkflowBoundsOnlyTheDebouncedWorkflow() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    WorkflowHandle<String, RuntimeException> withTimeout;
    try (var o = new WorkflowOptions().withTimeout(Duration.ofMinutes(2)).setContext()) {
      withTimeout =
          dbos.<String>debouncer()
              .debounce("ambient-timeout", Duration.ofMillis(200), () -> svc.process("a"));
    }
    WorkflowHandle<String, RuntimeException> withDeadline;
    try (var o =
        new WorkflowOptions().withDeadline(Instant.now().plus(Duration.ofHours(1))).setContext()) {
      withDeadline =
          dbos.<String>debouncer()
              .debounce("ambient-deadline", Duration.ofMillis(200), () -> svc.process("b"));
    }
    assertEquals("result:a", withTimeout.getResult());
    assertEquals("result:b", withDeadline.getResult());

    assertEquals(
        0, DebouncedRows.countByName(pgContainer.dataSource(), Constants.DEBOUNCER_WORKFLOW_NAME));

    var timed = withTimeout.getStatus();
    assertEquals(Duration.ofMinutes(2).toMillis(), timed.timeoutMs());
    assertNotNull(timed.deadlineEpochMs());
    var deadlined = withDeadline.getStatus();
    assertNull(deadlined.timeoutMs());
    assertNull(deadlined.deadlineEpochMs());
  }

  public interface PortableOrchestratorService {
    String debounceTwice(String arg);
  }

  public static class PortableOrchestratorServiceImpl implements PortableOrchestratorService {
    private final DBOS dbos;
    private final DebouncedService svc;

    public PortableOrchestratorServiceImpl(DBOS dbos, DebouncedService svc) {
      this.dbos = dbos;
      this.svc = svc;
    }

    @Override
    @Workflow(serializationStrategy = SerializationStrategy.PORTABLE)
    public String debounceTwice(String arg) {
      var debouncer = dbos.<String>debouncer();
      // The first call creates the debounced workflow; the second bounces it with its own args.
      debouncer.debounce("portable-debounce", Duration.ofMillis(800), () -> svc.process("first"));
      return debouncer
          .debounce("portable-debounce", Duration.ofMillis(800), () -> svc.process(arg))
          .getResult();
    }
  }

  // The debounced workflow's inputs take the debounced workflow's format, not the caller's.
  @Test
  public void debounceFromAPortableWorkflowDeliversItsControlMessage() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch =
        dbos.registerProxy(
            PortableOrchestratorService.class, new PortableOrchestratorServiceImpl(dbos, svc));
    dbos.launch();

    var h = dbos.startWorkflow(() -> orch.debounceTwice("second"));
    assertEquals("result:second", h.getResult());
    assertEquals(1, serviceImpl.callCount());

    assertEquals(List.of("second"), serviceImpl.callArgs());
  }

  // Verify that a second debounce call after the first window closes starts a fresh window.
  // Regression test for: deduplication_id is cleared to NULL on completion, so the UNIQUE
  // constraint no longer blocks a new enqueue with the same key.
  @Test
  public void reDebounceAfterWindowCloses() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var debouncer = dbos.<String>debouncer();

    // First window
    var h1 = debouncer.debounce("rekey", Duration.ofMillis(400), () -> svc.process("first"));
    assertEquals("result:first", h1.getResult());
    assertEquals(1, serviceImpl.callCount());

    // Wait long enough to ensure the first debouncer workflow has completed.
    Thread.sleep(300);

    // Second window — must NOT livelock; must start a fresh debouncer.
    var h2 = debouncer.debounce("rekey", Duration.ofMillis(400), () -> svc.process("second"));
    assertEquals("result:second", h2.getResult());
    assertEquals(2, serviceImpl.callCount());

    // Each window produces an independent user workflow.
    assertNotEquals(h1.workflowId(), h2.workflowId());
  }

  // Recovering a workflow that debounced replays the child slot, which returns the workflow it
  // enqueued: no second debounced workflow is created.
  @Test
  public void recoveryDoesNotEnqueueTheDebouncedWorkflowAgain() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch =
        dbos.registerProxy(JoiningOrchestrator.class, new JoiningOrchestratorImpl(dbos, svc));
    dbos.launch();

    var orchestratorId = "wf-recover-orchestrator";
    String userWorkflowId;
    try (var o = new WorkflowOptions(orchestratorId).setContext()) {
      userWorkflowId = orch.joinDebounce("rec-key", "v1");
    }

    flipToPending(orchestratorId);
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var recovered = executor.recoverPendingWorkflows(List.of(executor.executorId()));
    assertTrue(recovered.contains(orchestratorId), "orchestrator was not recovered");
    assertEquals(userWorkflowId, dbos.retrieveWorkflow(orchestratorId).getResult());

    assertEquals(1, countWorkflowsByName("process"));
    assertEquals(
        WorkflowState.DELAYED.name(),
        DebouncedRows.read(pgContainer.dataSource(), userWorkflowId).status());
  }

  private int countWorkflowsByName(String name) throws SQLException {
    var sql = "SELECT count(*) FROM dbos.workflow_status WHERE name = ?";
    try (Connection conn = pgContainer.dataSource().getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, name);
      try (var rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  // withDeduplicationId is ignored: the debounced workflow holds its debounce key there.
  @Test
  @SuppressWarnings("removal")
  public void ignoresTheDeduplicationId() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    String userQueue = "dedup-user-queue";
    dbos.launch();
    dbos.registerQueue(userQueue, new QueueOptions());

    var handle =
        dbos.<String>debouncer()
            .withQueue(userQueue)
            .withDeduplicationId("user-dedup-1")
            .debounce("dd-key", Duration.ofSeconds(1), () -> svc.process("v1"));

    assertEquals(
        "process-dd-key",
        DebouncedRows.read(pgContainer.dataSource(), handle.workflowId()).deduplicationId());
    assertEquals("result:v1", handle.getResult());
    assertEquals(1, serviceImpl.callCount());
  }

  // ==================== The debounced workflow's row ====================

  @Test
  public void writesTheDebouncedWorkflowDelayedOnTheInternalQueue() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    var dataSource = pgContainer.dataSource();

    long before = System.currentTimeMillis();
    var first =
        dbos.<String>debouncer().debounce("row", Duration.ofSeconds(30), () -> svc.process("a"));
    var row = DebouncedRows.read(dataSource, first.workflowId());
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    assertEquals(Constants.DBOS_INTERNAL_QUEUE, row.queueName());
    assertEquals("process-row", row.deduplicationId());
    assertTrue(row.isDebounced());
    assertNull(row.debounceDeadlineEpochMs());
    assertTrue(row.delayUntilEpochMs() >= before + 30_000, "delay " + row.delayUntilEpochMs());
    assertEquals(0, DebouncedRows.countByName(dataSource, Constants.DEBOUNCER_WORKFLOW_NAME));

    // The next call bounces that row: same workflow, its inputs replaced.
    var second =
        dbos.<String>debouncer().debounce("row", Duration.ofSeconds(30), () -> svc.process("b"));
    assertEquals(first.workflowId(), second.workflowId());
    assertEquals(1, countWorkflowsByName("process"));
  }

  @Test
  public void writesTheDebouncedWorkflowOnTheUserQueueWithItsPriority() throws Exception {
    String userQueue = "row-user-queue";
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    dbos.registerQueue(userQueue, new QueueOptions());

    var handle =
        dbos.<String>debouncer()
            .withQueue(userQueue)
            .withPriority(7)
            .debounce("row", Duration.ofSeconds(30), () -> svc.process("a"));

    var row = DebouncedRows.read(pgContainer.dataSource(), handle.workflowId());
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    assertEquals(userQueue, row.queueName());
    assertEquals("process-row", row.deduplicationId());
    assertTrue(row.isDebounced());
    assertEquals(7, row.priority());
  }

  @Test
  public void capsTheDelayAtTheDebounceDeadline() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    var dataSource = pgContainer.dataSource();
    var debouncer = dbos.<String>debouncer().withDebounceTimeout(Duration.ofSeconds(20));

    long before = System.currentTimeMillis();
    var handle = debouncer.debounce("cap", Duration.ofMinutes(5), () -> svc.process("a"));
    long after = System.currentTimeMillis();

    var row = DebouncedRows.read(dataSource, handle.workflowId());
    assertNotNull(row.debounceDeadlineEpochMs());
    assertTrue(row.debounceDeadlineEpochMs() >= before + 20_000);
    assertTrue(row.debounceDeadlineEpochMs() <= after + 20_000);
    assertEquals(row.debounceDeadlineEpochMs(), row.delayUntilEpochMs());

    // A bounce cannot push it past the deadline either.
    debouncer.debounce("cap", Duration.ofMinutes(5), () -> svc.process("b"));
    assertEquals(
        row.debounceDeadlineEpochMs(),
        DebouncedRows.read(dataSource, handle.workflowId()).delayUntilEpochMs());
  }

  @Test
  public void aPartitionedQueueFailsAtTheCall() throws Exception {
    String partitioned = "partitioned-queue";
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    dbos.registerQueue(partitioned, new QueueOptions().withPartitionConcurrency(1));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.<String>debouncer()
                .withQueue(partitioned)
                .debounce("part", Duration.ofSeconds(1), () -> svc.process("a")));
    assertEquals(0, countWorkflowsByName("process"));
  }

  // ==================== Service workflows of SDK versions before 1.2 ====================
  //
  // Before 1.2 a service workflow on the internal queue held the key, absorbed calls over messages
  // and started the user workflow when the period elapsed. The service workflow is still
  // registered, so a planted row under this executor's version is run here exactly as a live node
  // of that version would run it; under a version nobody serves it is stranded.

  private String plantService(String key, String promisedId, String queue, String appVersion)
      throws SQLException {
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var serializer = DBOSTestAccess.getSystemDatabase(dbos).serializer();
    var options =
        new DebouncerOptions(
            "process", DebouncedServiceImpl.class.getName(), null, queue, null, null, null, null);
    var ctx = new DebouncerContextOptions(promisedId, null, null);
    var initial =
        new DebouncerMessage(
            UUID.randomUUID().toString(), new Object[] {"stale"}, Duration.ofSeconds(2));
    var inputs =
        SerializationUtil.serializeArgs(
            new Object[] {options, ctx, initial}, null, null, serializer);
    return DebouncedRows.insertService(
        pgContainer.dataSource(),
        "process-" + key,
        inputs.serializedValue(),
        inputs.serialization(),
        appVersion,
        executor.appName());
  }

  private String liveVersion() {
    return DBOSTestAccess.getDbosExecutor(dbos).appVersion();
  }

  @Test
  public void forwardsToALiveServiceWorkflowOnTheInternalQueue() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    var service = plantService("svc", "promised-1", null, liveVersion());

    var handle =
        dbos.<String>debouncer()
            .debounce("svc", Duration.ofMillis(300), () -> svc.process("fresh"));

    // The service workflow took our arguments and started the workflow it promised with them.
    assertEquals("promised-1", handle.workflowId());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), serviceImpl.callArgs());
    assertEquals(WorkflowState.SUCCESS, dbos.retrieveWorkflow(service).getStatus().status());
    assertEquals(1, countWorkflowsByName("process"));
  }

  @Test
  public void forwardsToALiveServiceWorkflowForAUserQueue() throws Exception {
    String userQueue = "svc-user-queue";
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    dbos.registerQueue(userQueue, new QueueOptions());
    plantService("svc", "promised-2", userQueue, liveVersion());

    // The new shape holds keys on the user queue, the service workflow on the internal one; the
    // first step looks there too before creating anything.
    var handle =
        dbos.<String>debouncer()
            .withQueue(userQueue)
            .debounce("svc", Duration.ofMillis(300), () -> svc.process("fresh"));

    assertEquals("promised-2", handle.workflowId());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(userQueue, dbos.getWorkflowStatus("promised-2").orElseThrow().queueName());
    assertEquals(1, countWorkflowsByName("process"));
  }

  @Test
  public void takesOverAStrandedServiceWorkflowUnderItsPromisedId() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    var dataSource = pgContainer.dataSource();
    var stranded = plantService("stranded", "promised-3", null, "no-such-version");

    long start = System.currentTimeMillis();
    var handle =
        dbos.<String>debouncer()
            .debounce("stranded", Duration.ofMillis(300), () -> svc.process("fresh"));
    long waited = System.currentTimeMillis() - start;

    // Bounded: a few ack timeouts, not forever.
    assertTrue(waited < 30_000, "took over after " + waited + "ms");
    // Cancelling it freed the key, and the workflow it promised is created here, so handles
    // earlier callers were given resolve to the workflow that really runs.
    assertEquals("promised-3", handle.workflowId());
    assertEquals(WorkflowState.CANCELLED, dbos.retrieveWorkflow(stranded).getStatus().status());
    var row = DebouncedRows.read(dataSource, "promised-3");
    assertTrue(row.isDebounced());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), serviceImpl.callArgs());
  }

  @Test
  public void cancelsAStrandedServiceWorkflowThatAlreadyStartedItsWorkflow() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    // Its node started the promised workflow, then died before the service workflow finished, so
    // it holds the key under a version nothing serves.
    var stranded = plantService("started", "promised-8", null, "no-such-version");
    dbos.startWorkflow(
            () -> svc.process("from-service"),
            new StartWorkflowOptions().withWorkflowId("promised-8"))
        .getResult();

    var handle =
        dbos.<String>debouncer()
            .debounce("started", Duration.ofMillis(300), () -> svc.process("fresh"));

    // The cancel frees the key; that workflow already ran, so this call starts its own.
    assertEquals(WorkflowState.CANCELLED, dbos.retrieveWorkflow(stranded).getStatus().status());
    assertNotEquals("promised-8", handle.workflowId());
    assertEquals("result:fresh", handle.getResult());
    assertEquals(2, serviceImpl.callCount());
  }

  @Test
  public void startsOverWhenASlowServiceWorkflowStartsThePromisedWorkflowFirst() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    var slow = plantService("slow", "promised-4", null, "no-such-version");

    // A node of the old version was alive after all: between the cancel and the create, its
    // service workflow starts the promised workflow with the arguments it had.
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_DEBOUNCE_TAKEOVER,
        new DebugTriggers.DebugAction()
            .setCallback(
                () ->
                    dbos.startWorkflow(
                        () -> svc.process("from-service"),
                        new StartWorkflowOptions().withWorkflowId("promised-4"))));
    WorkflowHandle<String, RuntimeException> handle;
    try {
      handle =
          dbos.<String>debouncer()
              .debounce("slow", Duration.ofMillis(300), () -> svc.process("fresh"));
    } finally {
      DebugTriggers.clearDebugTriggers();
    }

    // This call's arguments did not go into that workflow, so it starts one of its own rather than
    // hand back a handle to a run that never sees them.
    assertNotEquals("promised-4", handle.workflowId());
    assertEquals("result:fresh", handle.getResult());
    assertEquals("result:from-service", dbos.retrieveWorkflow("promised-4").getResult());
    assertEquals(WorkflowState.CANCELLED, dbos.retrieveWorkflow(slow).getStatus().status());
    assertEquals(2, serviceImpl.callCount());
  }

  public interface JoiningOrchestrator {
    String joinDebounce(String key, String arg);

    String joinDebounceOn(String queue, String key, String arg);
  }

  public static class JoiningOrchestratorImpl implements JoiningOrchestrator {
    private final DBOS dbos;
    private final DebouncedService svc;

    public JoiningOrchestratorImpl(DBOS dbos, DebouncedService svc) {
      this.dbos = dbos;
      this.svc = svc;
    }

    @Override
    @Workflow
    public String joinDebounce(String key, String arg) {
      return dbos.<String>debouncer()
          .debounce(key, Duration.ofSeconds(5), () -> svc.process(arg))
          .workflowId();
    }

    @Override
    @Workflow
    public String joinDebounceOn(String queue, String key, String arg) {
      return dbos.<String>debouncer()
          .withQueue(queue)
          .debounce(key, Duration.ofSeconds(5), () -> svc.process(arg))
          .workflowId();
    }
  }

  @Test
  public void recordsTheServiceWorkflowInTheFirstStep() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch =
        dbos.registerProxy(JoiningOrchestrator.class, new JoiningOrchestratorImpl(dbos, svc));
    dbos.launch();
    var service = plantService("shape-key", "promised-5", null, liveVersion());

    var orchestratorId = "wf-shape-orchestrator";
    String joined;
    try (var o = new WorkflowOptions(orchestratorId).setContext()) {
      joined = orch.joinDebounce("shape-key", "second");
    }

    assertEquals("promised-5", joined);
    var recorded = recordedStep(orchestratorId, "DBOS.assignDebounceIds");
    assertTrue(recorded.output().contains(service), "recorded: " + recorded.output());
  }

  /**
   * A bounce from inside a workflow is recorded with its checkpoint in one transaction, and a
   * replay returns the recorded result rather than bouncing again: the extended row keeps the delay
   * the first run gave it.
   */
  @Test
  public void replaysABounceRecordedInsideAWorkflowWithoutBouncingAgain() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch =
        dbos.registerProxy(JoiningOrchestrator.class, new JoiningOrchestratorImpl(dbos, svc));
    dbos.launch();
    var dataSource = pgContainer.dataSource();
    long planted = System.currentTimeMillis() + 60_000;
    var waiting =
        DebouncedRows.insert(
            dataSource, debouncedRow(Constants.DBOS_INTERNAL_QUEUE, "process", planted));

    var orchestratorId = "wf-bounce-orchestrator";
    String bounced;
    try (var o = new WorkflowOptions(orchestratorId).setContext()) {
      bounced = orch.joinDebounce("mixed", "second");
    }
    assertEquals(waiting, bounced);
    var afterBounce = DebouncedRows.read(dataSource, waiting).delayUntilEpochMs();
    assertTrue(afterBounce < planted);
    var recorded = recordedStep(orchestratorId, "DBOS.assignDebounceIds");
    assertTrue(recorded.output().contains(waiting), "recorded: " + recorded.output());

    // Replay it. The first step returns its recorded ids, so the row is not touched again.
    flipToPending(orchestratorId);
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var recovered = executor.recoverPendingWorkflows(List.of(executor.executorId()));
    assertTrue(recovered.contains(orchestratorId), "orchestrator was not recovered");
    assertEquals(waiting, dbos.retrieveWorkflow(orchestratorId).getResult());

    assertEquals(afterBounce, DebouncedRows.read(dataSource, waiting).delayUntilEpochMs());
    assertEquals(0, DebouncedRows.countByName(dataSource, Constants.DEBOUNCER_WORKFLOW_NAME));
  }

  // ==================== Replaying what 1.1 recorded ====================
  //
  // A workflow that debounced under 1.1 and replays here -- which only happens when the application
  // version is pinned across the upgrade, since the SDK version is otherwise hashed into the
  // computed version and recovery only claims workflows matching it -- must resume. Each test runs
  // an orchestrator for real, then swaps its recorded steps for the ones 1.1 wrote.

  /** The ids 1.1's first step recorded: three components, under this database's serializer. */
  private DebouncedRows.Step legacyIds(String userWorkflowId, String messageId) {
    var serialized =
        SerializationUtil.serializeValue(
            new Debouncer.DebounceIds(userWorkflowId, messageId, null, null),
            null,
            DBOSTestAccess.getSystemDatabase(dbos).serializer());
    var output = serialized.serializedValue();
    var legacy = output.replace(",\"serviceWorkflowId\":null", "");
    assertNotEquals(output, legacy, "not the expected encoding: " + output);
    return new DebouncedRows.Step(legacy, serialized.serialization());
  }

  private DebouncedRows.Step recordedValue(Object value) {
    var serialized =
        SerializationUtil.serializeValue(
            value, null, DBOSTestAccess.getSystemDatabase(dbos).serializer());
    return new DebouncedRows.Step(serialized.serializedValue(), serialized.serialization());
  }

  /** Runs the orchestrator, then clears what it did so its recording can be replaced. */
  private void runAndClear(
      String orchestratorId, JoiningOrchestrator orch, String queue, String key)
      throws SQLException {
    String created;
    try (var o = new WorkflowOptions(orchestratorId).setContext()) {
      created =
          queue == null
              ? orch.joinDebounce(key, "second")
              : orch.joinDebounceOn(queue, key, "second");
    }
    var dataSource = pgContainer.dataSource();
    DebouncedRows.deleteWorkflow(dataSource, created);
    DebouncedRows.deleteSteps(dataSource, orchestratorId);
  }

  private String replay(String orchestratorId) throws Exception {
    flipToPending(orchestratorId);
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var recovered = executor.recoverPendingWorkflows(List.of(executor.executorId()));
    assertTrue(recovered.contains(orchestratorId), "orchestrator was not recovered");
    WorkflowHandle<String, RuntimeException> handle = dbos.retrieveWorkflow(orchestratorId);
    return handle.getResult();
  }

  /**
   * 1.1 recorded the service workflow it started in the child slot. That slot now holds the
   * debounced workflow's own enqueue, and replay returns the workflow the service workflow
   * promised.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void replaysADebounceThatStartedAServiceWorkflow(boolean userQueue) throws Exception {
    String queue = userQueue ? "legacy-user-queue" : null;
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch =
        dbos.registerProxy(JoiningOrchestrator.class, new JoiningOrchestratorImpl(dbos, svc));
    dbos.launch();
    if (queue != null) {
      dbos.registerQueue(queue, new QueueOptions());
    }
    var dataSource = pgContainer.dataSource();
    var orchestratorId = "wf-legacy-fresh-" + userQueue;
    runAndClear(orchestratorId, orch, queue, "legacy");

    var service = plantService("legacy", "promised-6", queue, "no-such-version");
    var ids = legacyIds("promised-6", "msg-6");
    DebouncedRows.insertStep(
        dataSource,
        orchestratorId,
        0,
        "DBOS.assignDebounceIds",
        ids.output(),
        ids.serialization(),
        null);
    DebouncedRows.insertStep(
        dataSource, orchestratorId, 1, Constants.DEBOUNCER_WORKFLOW_NAME, null, null, service);

    assertEquals("promised-6", replay(orchestratorId));
    assertEquals(0, countWorkflowsByName("process"));
  }

  /**
   * 1.1's join path: its enqueue collided and recorded nothing, DBOS.lookupDebouncer recorded the
   * service workflow, and the send and two getEvent steps followed. The replay's enqueue collides
   * again with the service workflow still holding the key, and every step after it replays. 1.0
   * recorded the lookup as the bare workflow id.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void replaysADebounceThatJoinedAServiceWorkflow(boolean bareId) throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch =
        dbos.registerProxy(JoiningOrchestrator.class, new JoiningOrchestratorImpl(dbos, svc));
    dbos.launch();
    var dataSource = pgContainer.dataSource();
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var orchestratorId = "wf-legacy-join-" + bareId;
    runAndClear(orchestratorId, orch, null, "joined");

    var service = plantService("joined", "promised-7", null, "no-such-version");
    var ids = legacyIds("never-created", "msg-7");
    var holder =
        recordedValue(
            bareId
                ? service
                : new DebounceResult.NotBounced(
                    new DeduplicationHolder(
                        service,
                        executor.appName(),
                        Constants.DEBOUNCER_WORKFLOW_NAME,
                        Constants.DEBOUNCER_CLASS_NAME,
                        null,
                        WorkflowState.ENQUEUED,
                        false)));
    var sent = recordedValue(null);
    var ack = recordedValue("msg-7");
    var child = recordedValue("promised-7");
    DebouncedRows.insertStep(
        dataSource,
        orchestratorId,
        0,
        "DBOS.assignDebounceIds",
        ids.output(),
        ids.serialization(),
        null);
    DebouncedRows.insertStep(
        dataSource,
        orchestratorId,
        2,
        "DBOS.lookupDebouncer",
        holder.output(),
        holder.serialization(),
        null);
    DebouncedRows.insertStep(
        dataSource, orchestratorId, 3, "DBOS.send", sent.output(), sent.serialization(), null);
    DebouncedRows.insertStep(
        dataSource, orchestratorId, 4, "DBOS.getEvent", ack.output(), ack.serialization(), null);
    DebouncedRows.insertStep(
        dataSource,
        orchestratorId,
        6,
        "DBOS.getEvent",
        child.output(),
        child.serialization(),
        null);

    assertEquals("promised-7", replay(orchestratorId));
    assertEquals(0, countWorkflowsByName("process"));
    assertEquals(WorkflowState.ENQUEUED, dbos.retrieveWorkflow(service).getStatus().status());
  }

  // ==================== Coalescing into a debounced workflow ====================
  //
  // A newer SDK version keeps a debounced workflow waiting DELAYED on its queue, holding its
  // debounce key as its deduplication ID, and coalesces by extending that row. In a fleet mixing
  // that version with this one, this debouncer has to coalesce into such a row rather than start
  // a service workflow beside it.

  private DebouncedRows.Spec debouncedRow(String queue, String workflowName, long delayUntil) {
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var serializer = DBOSTestAccess.getSystemDatabase(dbos).serializer();
    var stale = SerializationUtil.serializeArgs(new Object[] {"stale"}, null, null, serializer);
    return new DebouncedRows.Spec(
        workflowName,
        DebouncedServiceImpl.class.getName(),
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
  public void coalescesIntoADebouncedWorkflowWaitingOnTheInternalQueue() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    var dataSource = pgContainer.dataSource();
    // Far enough out that the row is still waiting when the debounce reaches it.
    long planted = System.currentTimeMillis() + 60_000;
    var waiting =
        DebouncedRows.insert(
            dataSource, debouncedRow(Constants.DBOS_INTERNAL_QUEUE, "process", planted));
    // The writer also kept the inputs in the payload table, which the runner reads first.
    DebouncedRows.insertInput(
        dataSource, waiting, DebouncedRows.read(dataSource, waiting).inputs());

    var handle =
        dbos.<String>debouncer()
            .debounce("mixed", Duration.ofMillis(500), () -> svc.process("fresh"));

    // The bounce extended the waiting row: the handle is that row, its delay moved to our period
    // and its inputs are ours. No service workflow was started beside it.
    assertEquals(waiting, handle.workflowId());
    var bounced = DebouncedRows.read(dataSource, waiting);
    assertTrue(bounced.delayUntilEpochMs() < planted);
    assertEquals(0, DebouncedRows.countByName(dataSource, Constants.DEBOUNCER_WORKFLOW_NAME));

    // Once the delay elapses the sweep enqueues it, clearing the key, and it runs with our args.
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), serviceImpl.callArgs());
    assertNull(dbos.getWorkflowStatus(waiting).orElseThrow().deduplicationId());
  }

  @Test
  public void coalescesIntoADebouncedWorkflowWaitingOnAUserQueue() throws Exception {
    String userQueue = "mixed-user-queue";
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    dbos.registerQueue(userQueue, new QueueOptions());
    var dataSource = pgContainer.dataSource();
    long planted = System.currentTimeMillis() + 60_000;
    var waiting = DebouncedRows.insert(dataSource, debouncedRow(userQueue, "process", planted));

    var handle =
        dbos.<String>debouncer()
            .withQueue(userQueue)
            .debounce("mixed", Duration.ofMillis(500), () -> svc.process("fresh"));

    assertEquals(waiting, handle.workflowId());
    assertTrue(DebouncedRows.read(dataSource, waiting).delayUntilEpochMs() < planted);
    assertEquals(0, DebouncedRows.countByName(dataSource, Constants.DEBOUNCER_WORKFLOW_NAME));
    assertEquals("result:fresh", handle.getResult());
    assertEquals(List.of("fresh"), serviceImpl.callArgs());
    assertEquals(userQueue, dbos.getWorkflowStatus(waiting).orElseThrow().queueName());
  }

  @Test
  public void refusesAKeyHeldByADifferentDebouncedWorkflow() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();
    var dataSource = pgContainer.dataSource();
    long planted = System.currentTimeMillis() + 60_000;
    // "process" + "mixed" collides with "process-mixed" held for another workflow.
    var other =
        DebouncedRows.insert(
            dataSource, debouncedRow(Constants.DBOS_INTERNAL_QUEUE, "other", planted));

    assertThrows(
        DBOSQueueDuplicatedException.class,
        () ->
            dbos.<String>debouncer()
                .debounce("mixed", Duration.ofMillis(500), () -> svc.process("fresh")));

    // Untouched: the other workflow keeps its delay and its inputs.
    assertEquals(planted, DebouncedRows.read(dataSource, other).delayUntilEpochMs());
    assertEquals(0, serviceImpl.callCount());
  }

  @Test
  public void rejectsAPriorityWithoutAQueue() {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                dbos.<String>debouncer()
                    .withPriority(3)
                    .debounce("prio", Duration.ofMillis(500), () -> svc.process("x")));

    assertTrue(e.getMessage().contains("queue"), e.getMessage());
    assertEquals(0, serviceImpl.callCount());
  }

  private record RecordedStep(int functionId, String serialization, String output) {}

  private RecordedStep recordedStep(String workflowId, String name) throws SQLException {
    var sql =
        "SELECT function_id, serialization, output FROM dbos.operation_outputs"
            + " WHERE workflow_uuid = ? AND function_name = ?";
    try (Connection conn = pgContainer.dataSource().getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, name);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "no " + name + " step was recorded");
        return new RecordedStep(
            rs.getInt("function_id"), rs.getString("serialization"), rs.getString("output"));
      }
    }
  }

  private void flipToPending(String workflowId) throws SQLException {
    var sql =
        "UPDATE dbos.workflow_status SET status = ?, queue_name = NULL, updated_at = ?"
            + " WHERE workflow_uuid = ?";
    try (Connection conn = pgContainer.dataSource().getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.PENDING.name());
      stmt.setLong(2, Instant.now().toEpochMilli());
      stmt.setString(3, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }
  }
}
