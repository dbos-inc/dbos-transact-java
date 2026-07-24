package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.PgContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;

/**
 * Races between a cancelled run's in-flight terminal outcome write and a concurrent resume of the
 * same workflow. The test parks run 1's outcome UPDATE (via a JDBC proxy) to hold open the window
 * between the workflow function returning and its outcome becoming durable.
 */
public class CancelResumeRaceTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource realDataSource;
  @AutoClose DBOS dbos;

  // Latches coordinating the parked stale write.
  final CountDownLatch parked = new CountDownLatch(1);
  final CountDownLatch releaseStale = new CountDownLatch(1);
  final CountDownLatch staleDone = new CountDownLatch(1);

  RaceServiceImpl impl;
  RaceService proxy;

  static final String OUTCOME_SQL_MARKER = "SET status = ?, output = ?, error = ?";

  private static Object invokeUnwrapped(Method m, Object target, Object[] args) throws Throwable {
    try {
      return m.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }

  /**
   * Wraps a DataSource so that the FIRST outcome UPDATE writing status SUCCESS for {@code
   * targetWorkflowId} parks (signals {@code parked}, awaits {@code releaseStale}) before executing,
   * then signals {@code staleDone}. Everything else passes through.
   */
  private DataSource parkingDataSource(DataSource delegate, String targetWorkflowId) {
    var loader = getClass().getClassLoader();
    var armed = new AtomicBoolean(true);

    return (DataSource)
        Proxy.newProxyInstance(
            loader,
            new Class<?>[] {DataSource.class},
            (dsProxy, dsMethod, dsArgs) -> {
              Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
              if (!dsMethod.getName().equals("getConnection") || result == null) {
                return result;
              }
              Connection conn = (Connection) result;
              return Proxy.newProxyInstance(
                  loader,
                  new Class<?>[] {Connection.class},
                  (connProxy, connMethod, connArgs) -> {
                    Object stmt = invokeUnwrapped(connMethod, conn, connArgs);
                    if (!connMethod.getName().equals("prepareStatement")
                        || stmt == null
                        || connArgs == null
                        || connArgs.length == 0
                        || !(connArgs[0] instanceof String sql)
                        || !sql.contains(OUTCOME_SQL_MARKER)) {
                      return stmt;
                    }
                    PreparedStatement ps = (PreparedStatement) stmt;
                    String[] boundStatus = new String[1];
                    String[] boundWfId = new String[1];
                    return Proxy.newProxyInstance(
                        loader,
                        new Class<?>[] {PreparedStatement.class},
                        (psProxy, psMethod, psArgs) -> {
                          if (psMethod.getName().equals("setString")
                              && psArgs != null
                              && psArgs.length == 2) {
                            int idx = (Integer) psArgs[0];
                            if (idx == 1) boundStatus[0] = (String) psArgs[1];
                            if (idx == 6) boundWfId[0] = (String) psArgs[1];
                          }
                          boolean isTargetStaleWrite =
                              psMethod.getName().equals("executeUpdate")
                                  && "SUCCESS".equals(boundStatus[0])
                                  && targetWorkflowId.equals(boundWfId[0])
                                  && armed.compareAndSet(true, false);
                          if (!isTargetStaleWrite) {
                            return invokeUnwrapped(psMethod, ps, psArgs);
                          }
                          parked.countDown();
                          try {
                            assertTrue(
                                releaseStale.await(30, TimeUnit.SECONDS),
                                "parked stale outcome write was never released");
                            return invokeUnwrapped(psMethod, ps, psArgs);
                          } finally {
                            staleDone.countDown();
                          }
                        });
                  });
            });
  }

  private void setUp(String workflowId) {
    realDataSource = pgContainer.dataSource();
    var config =
        pgContainer.dbosConfig().withDataSource(parkingDataSource(realDataSource, workflowId));
    dbos = new DBOS(config);
    impl = new RaceServiceImpl();
    proxy = dbos.registerProxy(RaceService.class, impl);
    dbos.launch();
  }

  // Run the workflow to the point where run 1 has been cancelled, has returned, and its terminal
  // outcome write is parked.
  private void runCancelAndPark(String workflowId) throws Exception {
    dbos.startWorkflow(() -> proxy.raceWorkflow(), new StartWorkflowOptions(workflowId));
    assertTrue(impl.entered.await(15, TimeUnit.SECONDS), "run 1 never entered the workflow");

    // Cancel while run 1 is still executing; CANCELLED is durably written.
    dbos.cancelWorkflow(workflowId);

    // Let run 1's function return. Its terminal outcome write parks before executing.
    impl.releaseWorkflow.countDown();
    assertTrue(parked.await(15, TimeUnit.SECONDS), "run 1 outcome write never parked");

    // Durable status must be CANCELLED (written by cancel; the outcome write is parked).
    assertEquals(WorkflowState.CANCELLED, dbos.retrieveWorkflow(workflowId).getStatus().status());
  }

  @Test
  public void activeIdReleasedBeforeOutcomeWriteTest() throws Exception {
    // The executor's active-workflow-ID entry must be released BEFORE the terminal outcome write
    // becomes durable. Otherwise: run 1's stale write is in flight, a client observes CANCELLED
    // and resumes, this same executor dequeues the resumed workflow, but the dispatch finds the
    // stale active-ID entry and is rejected, leaving the row PENDING with nobody executing it.
    String workflowId = "activeIdReleasedBeforeOutcome:%d".formatted(System.currentTimeMillis());
    setUp(workflowId);
    DBOSTestAccess.getQueueService(dbos).setSpeedupForTest();

    runCancelAndPark(workflowId);

    WorkflowHandle<String, ?> resumedHandle = dbos.resumeWorkflow(workflowId);

    // While the stale write is still parked, the resumed workflow must be dequeued and executed
    // by this same executor.
    assertTrue(
        impl.secondRunDone.await(15, TimeUnit.SECONDS),
        "resumed dispatch was blocked by a stale active workflow ID");
    assertEquals("completed", resumedHandle.getResult());
    assertEquals(2, impl.runs.get());

    // Unblock the parked stale write so shutdown is not blocked.
    releaseStale.countDown();
    assertTrue(staleDone.await(15, TimeUnit.SECONDS), "stale outcome write never completed");
  }
}

interface RaceService {
  String raceWorkflow() throws InterruptedException;
}

class RaceServiceImpl implements RaceService {
  final AtomicInteger runs = new AtomicInteger();
  final CountDownLatch entered = new CountDownLatch(1);
  final CountDownLatch releaseWorkflow = new CountDownLatch(1);
  final CountDownLatch secondRunDone = new CountDownLatch(1);

  @Override
  @Workflow(name = "raceWorkflow")
  public String raceWorkflow() throws InterruptedException {
    if (runs.incrementAndGet() > 1) {
      secondRunDone.countDown();
      return "completed";
    }
    entered.countDown();
    if (!releaseWorkflow.await(30, TimeUnit.SECONDS)) {
      throw new IllegalStateException("run 1 was never released");
    }
    return "";
  }
}
