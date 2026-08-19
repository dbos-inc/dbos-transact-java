package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The polling limiter is a throughput guard, not a behavioural one, so what needs proving is that
 * it changes nothing when it binds hard. A limit of 1 serialises every polling read in the process;
 * the waits must still all finish, which they only do if no permit is ever held across a wait or
 * across another read that needs one.
 */
public class PollingConcurrencyTest {

  private static final int WAITERS = 8;
  private static final int VALUES = 5;

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  PollingService proxy;

  interface PollingService {
    void streamAndSignal(String key, int count);
  }

  @WorkflowClassName("PollingServiceImpl")
  static class PollingServiceImpl implements PollingService {
    private final DBOS dbos;

    PollingServiceImpl(DBOS dbos) {
      this.dbos = dbos;
    }

    @Workflow
    @Override
    public void streamAndSignal(String key, int count) {
      for (var i = 0; i < count; i++) {
        // Spread the writes out so the readers spend the run blocked rather than draining rows
        // that were already committed before they started.
        dbos.sleep(Duration.ofMillis(200));
        dbos.writeStream(key, "value" + i);
      }
      dbos.closeStream(key);
      dbos.setEvent(key, "done");
    }
  }

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig().withDatabasePollingConcurrency(1);
    dbos = new DBOS(dbosConfig);
    proxy = dbos.registerProxy(PollingService.class, new PollingServiceImpl(dbos));
    dbos.launch();
  }

  @Test
  @DisplayName("Concurrent waits all complete when only one polling read may run at a time")
  public void concurrentWaitsCompleteUnderAHardLimit() throws Exception {
    var wfid = UUID.randomUUID().toString();
    var key = "polling_key";

    // Start the producer first: reading a stream of a workflow that does not exist yet is an
    // error, in every SDK. It sleeps before its first write, so the waiters still spend the run
    // blocked, all contending for the single permit.
    var handle =
        dbos.startWorkflow(
            () -> proxy.streamAndSignal(key, VALUES), new StartWorkflowOptions(wfid));

    var start = new CountDownLatch(1);
    var done = new CountDownLatch(WAITERS * 2);
    var failures = new AtomicReference<Throwable>();
    var readers = new ArrayList<List<Object>>();
    var threads = new ArrayList<Thread>();

    for (var i = 0; i < WAITERS; i++) {
      var values = new ArrayList<>();
      readers.add(values);
      threads.add(
          waiter(
              start,
              done,
              failures,
              () -> {
                var iter = dbos.readStream(wfid, key);
                while (iter.hasNext()) {
                  values.add(iter.next());
                }
              }));
      threads.add(
          waiter(
              start,
              done,
              failures,
              () -> {
                var event = dbos.<String>getEvent(wfid, key, Duration.ofSeconds(60));
                assertEquals("done", event.orElse(null));
              }));
    }

    threads.forEach(Thread::start);
    start.countDown();
    handle.getResult();

    assertTrue(done.await(120, TimeUnit.SECONDS), "waiters did not all finish");
    if (failures.get() != null) {
      throw new AssertionError("a waiter failed", failures.get());
    }

    var expected = new ArrayList<String>();
    for (var i = 0; i < VALUES; i++) {
      expected.add("value" + i);
    }
    for (var values : readers) {
      assertEquals(expected, values);
    }
  }

  private static Thread waiter(
      CountDownLatch start,
      CountDownLatch done,
      AtomicReference<Throwable> failures,
      Runnable body) {
    var t =
        new Thread(
            () -> {
              try {
                start.await();
                body.run();
              } catch (Throwable e) {
                failures.compareAndSet(null, e);
              } finally {
                done.countDown();
              }
            });
    t.setDaemon(true);
    return t;
  }
}
