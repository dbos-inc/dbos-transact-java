package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    Queue userQueue = new Queue("debouncer-user-queue");
    dbos.registerQueue(userQueue);
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var debouncer = dbos.<String>debouncer().withQueue(userQueue);
    var handle = debouncer.debounce("user-q", Duration.ofMillis(500), () -> svc.process("queued"));
    assertEquals("result:queued", handle.getResult());

    var status = dbos.getWorkflowStatus(handle.workflowId()).orElseThrow();
    assertEquals(userQueue.name(), status.queueName());
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
    private final Queue userQueue;

    public OrchestratorServiceImpl(DBOS dbos, DebouncedService svc, Queue userQueue) {
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
    Queue q = new Queue("prio-queue").withPriorityEnabled(true);
    dbos.registerQueue(q);
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    var orch =
        dbos.registerProxy(OrchestratorService.class, new OrchestratorServiceImpl(dbos, svc, q));
    dbos.launch();

    var h = dbos.startWorkflow(() -> orch.debounceWithPriority("prio-val"));
    assertEquals("result:prio-val", h.getResult());

    var userWfStatus =
        dbos
            .listWorkflows(
                new ListWorkflowsInput().withQueueName(q.name()).withWorkflowName("process"))
            .stream()
            .findFirst()
            .orElse(null);
    assertNotNull(userWfStatus, "user workflow 'process' not found on queue " + q.name());
    assertEquals(Integer.valueOf(42), userWfStatus.priority());
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

  // Recovering/replaying the internal debouncer workflow must be idempotent: it reuses the
  // pre-assigned user workflow id and must not start a second user workflow execution.
  @Test
  public void recoveryDoesNotRestartUserWorkflow() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    dbos.launch();

    var handle =
        dbos.<String>debouncer()
            .debounce("rec-key", Duration.ofMillis(300), () -> svc.process("v1"));
    String userWorkflowId = handle.workflowId();
    assertEquals("result:v1", handle.getResult());
    assertEquals(1, serviceImpl.callCount());

    // Simulate a crash where the debouncer ran but did not durably record completion: flip only
    // the debouncer workflow back to PENDING (the user workflow stays SUCCESS) and recover it.
    // The debouncer finishes asynchronously after starting the user workflow, so wait until it is
    // SUCCESS before flipping — otherwise the flip would race its own completion.
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    awaitDebouncerFlippedToPending(Duration.ofSeconds(30));

    var recovered = executor.recoverPendingWorkflows(List.of(executor.executorId()));
    assertEquals(1, recovered.size());
    for (var h : recovered) {
      h.getResult();
    }

    // Replay reused the same user workflow id and did not run the user workflow again. The count
    // check is independent of timing: a second user workflow would create a row at enqueue/start
    // time, before it could execute, so it would be caught even if its body had not run yet.
    assertEquals(1, countWorkflowsByName("process"));
    assertEquals(1, serviceImpl.callCount());
    assertEquals(List.of("v1"), serviceImpl.callArgs());
    WorkflowHandle<String, Exception> userHandle = dbos.retrieveWorkflow(userWorkflowId);
    assertEquals("result:v1", userHandle.getResult());
    assertEquals(WorkflowState.SUCCESS, userHandle.getStatus().status());
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

  // withDeduplicationId must forward the id to the queued user workflow.
  @Test
  public void deduplicationIdForwardedToQueuedUserWorkflow() throws Exception {
    DebouncedService svc = dbos.registerProxy(DebouncedService.class, serviceImpl);
    Queue userQueue = new Queue("dedup-user-queue");
    dbos.registerQueue(userQueue);
    serviceImpl.gate = new CountDownLatch(1);
    dbos.launch();

    String dedupId = "user-dedup-1";
    var handle =
        dbos.<String>debouncer()
            .withQueue(userQueue)
            .withDeduplicationId(dedupId)
            .debounce("dd-key", Duration.ofMillis(300), () -> svc.process("v1"));

    // The user workflow blocks on the gate while running, so its deduplication_id is still set
    // (it is cleared only on completion). Wait for it to appear, then assert it was forwarded.
    String observed = awaitDeduplicationId(handle, Duration.ofSeconds(30));
    assertEquals(dedupId, observed);

    serviceImpl.gate.countDown();
    assertEquals("result:v1", handle.getResult());
    assertEquals(1, serviceImpl.callCount());
  }

  private String awaitDeduplicationId(WorkflowHandle<String, ?> handle, Duration timeout)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      try {
        var status = handle.getStatus();
        if (status != null && status.deduplicationId() != null) {
          return status.deduplicationId();
        }
      } catch (RuntimeException ignored) {
        // status row not present yet
      }
      Thread.sleep(50);
    }
    throw new AssertionError("user workflow deduplicationId not observed within timeout");
  }

  // Flip the (completed) debouncer workflow back to PENDING, retrying until it has reached SUCCESS
  // so the result is deterministic regardless of how the debouncer's async completion interleaves.
  private void awaitDebouncerFlippedToPending(Duration timeout) throws Exception {
    var sql =
        "UPDATE dbos.workflow_status SET status = ?, queue_name = NULL, updated_at = ?"
            + " WHERE name = ? AND status = ?";
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      try (Connection conn = pgContainer.dataSource().getConnection();
          PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, WorkflowState.PENDING.name());
        stmt.setLong(2, Instant.now().toEpochMilli());
        stmt.setString(3, Constants.DEBOUNCER_WORKFLOW_NAME);
        stmt.setString(4, WorkflowState.SUCCESS.name());
        if (stmt.executeUpdate() == 1) {
          return;
        }
      }
      Thread.sleep(50);
    }
    throw new AssertionError("debouncer workflow did not reach SUCCESS within timeout");
  }
}
