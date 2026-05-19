package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;

import java.time.Duration;
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

    @Override
    @Workflow
    public String process(String input) {
      callCount.incrementAndGet();
      callArgs.add(input);
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
}
