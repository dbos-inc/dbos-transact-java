package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.spi.TestEnterpriseProvider;
import dev.dbos.transact.utils.LogCapture;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import ch.qos.logback.classic.Level;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShutdownWaitTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  // The Conductor stop hook this test set, if any.
  Runnable onStop;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();
  }

  @AfterEach
  void afterEach() {
    // Only the hook this test set: the class's tests run concurrently.
    if (onStop != null && TestEnterpriseProvider.onStop == onStop) {
      TestEnterpriseProvider.onStop = () -> {};
    }
  }

  private String status(String workflowId) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = ?")) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next(), workflowId);
        return rs.getString(1);
      }
    }
  }

  private record Launched(DBOS dbos, StepWaiter impl, StepWaiterService proxy) {}

  private Launched launch(boolean released) {
    var instance = new DBOS(dbosConfig);
    var impl = new StepWaiter(instance, released);
    var proxy = instance.registerProxy(StepWaiterService.class, impl);
    impl.setSelf(proxy);
    instance.launch();
    instance.registerQueue("drain-q", new QueueOptions());
    return new Launched(instance, impl, proxy);
  }

  @Test
  void aWorkflowMidStepCompletesDuringTheWait() throws Exception {
    var launched = launch(false);
    dbos = launched.dbos();
    var handle = dbos.startWorkflow(() -> launched.proxy().slow());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    var release = new Thread(() -> sleepThen(1_000, launched.impl().release::countDown));
    release.start();

    dbos.shutdown(Duration.ofSeconds(10));

    assertEquals("SUCCESS", status(handle.workflowId()));
  }

  @Test
  void nothingIsDequeuedOnceTheShutdownBegins() throws Exception {
    var launched = launch(false);
    dbos = launched.dbos();
    var slow = dbos.startWorkflow(() -> launched.proxy().slow());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    var enqueued = new String[1];
    var during =
        new Thread(
            () ->
                sleepThen(
                    1_000,
                    () -> {
                      enqueued[0] =
                          dbos.startWorkflow(
                                  () -> launched.proxy().quick(),
                                  new StartWorkflowOptions().withQueue("drain-q"))
                              .workflowId();
                      sleepThen(1_500, launched.impl().release::countDown);
                    }));
    during.start();

    dbos.shutdown(Duration.ofSeconds(10));
    during.join(10_000);

    assertEquals("SUCCESS", status(slow.workflowId()));
    assertEquals("ENQUEUED", status(enqueued[0]));
  }

  @Test
  void aWorkflowLongerThanTheWaitIsInterruptedAndRecoveredLater() throws Exception {
    var launched = launch(false);
    var handle = launched.dbos().startWorkflow(() -> launched.proxy().slow());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));

    long started = System.nanoTime();
    launched.dbos().shutdown(Duration.ofSeconds(1));
    var took = Duration.ofNanos(System.nanoTime() - started);
    assertTrue(took.compareTo(Duration.ofMillis(900)) >= 0, took.toString());
    assertTrue(took.compareTo(Duration.ofSeconds(8)) < 0, took.toString());
    // Abandoned unmarked, for recovery.
    assertEquals("PENDING", status(handle.workflowId()));

    var relaunched = launch(true);
    dbos = relaunched.dbos();
    assertEquals(
        "done", dbos.<String, RuntimeException>retrieveWorkflow(handle.workflowId()).getResult());
  }

  @Test
  void aShutdownWithoutATimeoutDoesNotWait() throws Exception {
    var launched = launch(false);
    var handle = launched.dbos().startWorkflow(() -> launched.proxy().slow());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));

    long started = System.nanoTime();
    launched.dbos().shutdown();
    // The workflow never finishes on its own, so a shutdown that waited for it would not return.
    assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(5)) < 0);
    assertEquals("PENDING", status(handle.workflowId()));
    // A second shutdown does nothing.
    launched.dbos().shutdown(Duration.ofSeconds(5));
  }

  // A read that fails as the pool fails one whose thread is interrupted while it waits.
  private DataSource interruptingOnce(AtomicReference<Thread> victim) {
    return (DataSource)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")
                  && victim.compareAndSet(Thread.currentThread(), null)) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted during connection acquisition");
              }
              return method.invoke(dataSource, args);
            });
  }

  @Test
  void anInterruptedDispatchRecordsNoError() throws Exception {
    var victim = new AtomicReference<Thread>();
    dbosConfig = dbosConfig.withDataSource(interruptingOnce(victim));
    var launched = launch(false);
    dbos = launched.dbos();
    var handle = dbos.startWorkflow(() -> launched.proxy().slow());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    var exec = DBOSTestAccess.getDbosExecutor(dbos);
    var ownerXid =
        DBTestAccess.takeOwnership(DBOSTestAccess.getSystemDatabase(dbos), handle.workflowId());

    victim.set(Thread.currentThread());
    assertThrows(
        RuntimeException.class, () -> exec.executeWorkflowById(handle.workflowId(), ownerXid));

    assertTrue(Thread.interrupted(), "the interrupt was swallowed");
    assertEquals("PENDING", status(handle.workflowId()));
    launched.impl().release.countDown();
  }

  @Test
  void aSecondShutdownDuringTheWaitLeavesTheWorkflowsRunning() throws Exception {
    var launched = launch(false);
    dbos = launched.dbos();
    var exec = DBOSTestAccess.getDbosExecutor(dbos);
    var handle = dbos.startWorkflow(() -> launched.proxy().slowThenStep());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    var first = new Thread(() -> dbos.shutdown(Duration.ofSeconds(20)));
    first.start();
    Thread.sleep(500);

    long started = System.nanoTime();
    dbos.shutdown();
    dbos.shutdown(Duration.ofSeconds(20));
    assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(2)) < 0);
    // Nor does a launch start another executor beside the one shutting down.
    dbos.launch();
    assertSame(exec, DBOSTestAccess.getDbosExecutor(dbos));
    assertTrue(first.isAlive());

    launched.impl().release.countDown();
    first.join(20_000);
    assertFalse(first.isAlive());
    assertEquals("SUCCESS", status(handle.workflowId()));
    assertNull(DBOSTestAccess.getDbosExecutor(dbos));
    dbos.launch();
    assertNotSame(exec, DBOSTestAccess.getDbosExecutor(dbos));
  }

  @Test
  void aWorkflowStillRunningAfterTheWaitCanEnqueueUntilTheDatabaseCloses() throws Exception {
    dbosConfig = dbosConfig.withConductorKey("test-conductor-key");
    var launched = launch(false);
    dbos = launched.dbos();
    var handle = dbos.startWorkflow(() -> launched.proxy().slowThenEnqueue());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    // The Conductor connection stops after the wait and before the system database closes: the
    // workflow is released there, and the shutdown is held until it has finished. The shutdown
    // runs on this thread; another test's, on its own thread, passes straight through.
    var testThread = Thread.currentThread();
    onStop =
        () -> {
          if (Thread.currentThread() != testThread) {
            return;
          }
          launched.impl().release.countDown();
          long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
          try {
            while (System.nanoTime() - deadline < 0
                && status(handle.workflowId()).equals("PENDING")) {
              Thread.sleep(20);
            }
          } catch (SQLException | InterruptedException e) {
            throw new IllegalStateException(e);
          }
        };
    TestEnterpriseProvider.onStop = onStop;
    var other = new Thread(onStop);
    other.start();
    other.join();
    assertEquals(1, launched.impl().release.getCount(), "another thread's stop ran the hook");

    dbos.shutdown(Duration.ofMillis(300));

    assertEquals("SUCCESS", status(handle.workflowId()));
  }

  @Test
  void aWaitLongerThanNanosecondsHoldWaits() throws Exception {
    var launched = launch(false);
    dbos = launched.dbos();
    var handle = dbos.startWorkflow(() -> launched.proxy().slow());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    var release = new Thread(() -> sleepThen(1_000, launched.impl().release::countDown));
    release.start();

    dbos.shutdown(Duration.ofSeconds(Long.MAX_VALUE));

    assertEquals("SUCCESS", status(handle.workflowId()));
  }

  @Test
  void anInterruptedWaitReportsTheWorkflowsStillRunning() throws Exception {
    var launched = launch(false);
    var handle = launched.dbos().startWorkflow(() -> launched.proxy().slow());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    try (var logs = new LogCapture(DBOSExecutor.class)) {
      var waiter = new Thread(() -> launched.dbos().shutdown(Duration.ofSeconds(60)));
      waiter.start();
      Thread.sleep(500);
      waiter.interrupt();
      waiter.join(20_000);
      assertFalse(waiter.isAlive());
      assertTrue(
          logs.messages(Level.WARN, "still active").stream()
              .anyMatch(m -> m.contains(handle.workflowId())),
          "the running workflow was not reported");
    }
    assertEquals("PENDING", status(handle.workflowId()));
  }

  @Test
  void aRunItsOwnTimeoutCancelledIsWaitedFor() throws Exception {
    var launched = launch(false);
    dbos = launched.dbos();
    dbos.startWorkflow(
        () -> launched.proxy().slow(),
        new StartWorkflowOptions().withTimeout(Duration.ofMillis(1_500)));
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    // The deadline has passed and the handle is done, while the run's code still runs.
    Thread.sleep(2_500);
    var release = new Thread(() -> sleepThen(1_500, launched.impl().release::countDown));
    release.start();

    long started = System.nanoTime();
    dbos.shutdown(Duration.ofSeconds(20));
    var took = Duration.ofNanos(System.nanoTime() - started);
    assertTrue(took.compareTo(Duration.ofMillis(1_200)) > 0, took.toString());
    assertTrue(launched.impl().left.await(0, TimeUnit.SECONDS), "its code was still running");
  }

  @Test
  void aRunAwaitingAnotherExecutionsOutcomeIsNotWaitedFor() throws Exception {
    var launched = launch(false);
    dbos = launched.dbos();
    var handle = dbos.startWorkflow(() -> launched.proxy().slow());
    assertTrue(launched.impl().inStep.await(10, TimeUnit.SECONDS));
    // Another execution owns the workflow now, so this one's outcome is refused and it waits.
    DBTestAccess.takeOwnership(DBOSTestAccess.getSystemDatabase(dbos), handle.workflowId());
    launched.impl().release.countDown();
    Thread.sleep(1_000);

    long started = System.nanoTime();
    dbos.shutdown(Duration.ofSeconds(20));
    var took = Duration.ofNanos(System.nanoTime() - started);
    assertTrue(took.compareTo(Duration.ofSeconds(10)) < 0, took.toString());
    assertEquals("PENDING", status(handle.workflowId()));
  }

  @Test
  void aNegativeTimeoutIsRefused() {
    dbos = new DBOS(dbosConfig);
    assertThrows(IllegalArgumentException.class, () -> dbos.shutdown(Duration.ofSeconds(-1)));
  }

  private static void sleepThen(long millis, Runnable then) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      return;
    }
    then.run();
  }
}

interface StepWaiterService {
  String slow();

  String slowThenStep();

  String slowThenEnqueue();

  String quick();
}

class StepWaiter implements StepWaiterService {

  final CountDownLatch inStep = new CountDownLatch(1);
  final CountDownLatch release;
  final CountDownLatch left = new CountDownLatch(1);
  private final DBOS dbos;
  private StepWaiterService self;

  StepWaiter(DBOS dbos, boolean released) {
    this.dbos = dbos;
    this.release = new CountDownLatch(released ? 0 : 1);
  }

  @Override
  @Workflow
  public String slow() {
    return waitInStep();
  }

  private String waitInStep() {
    return dbos.runStep(
        () -> {
          inStep.countDown();
          try {
            release.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
          }
          left.countDown();
          return "done";
        },
        "waitInStep");
  }

  void setSelf(StepWaiterService self) {
    this.self = self;
  }

  @Override
  @Workflow
  public String slowThenStep() {
    waitInStep();
    return dbos.runStep(() -> "second", "second");
  }

  @Override
  @Workflow
  public String slowThenEnqueue() {
    waitInStep();
    return dbos.startWorkflow(() -> self.quick(), new StartWorkflowOptions().withQueue("drain-q"))
        .workflowId();
  }

  @Override
  @Workflow
  public String quick() {
    return "quick";
  }
}
