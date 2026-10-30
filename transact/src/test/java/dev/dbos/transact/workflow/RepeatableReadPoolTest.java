package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.utils.PgContainer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * A tree cancel run on a pool whose connections default to REPEATABLE READ. Each level must read
 * its children after its own write has waited out a child's insert, which only READ COMMITTED
 * shows: a REPEATABLE READ snapshot, taken at the transaction's first statement, would hide the
 * child and let it escape. The step sets the level itself, so the pool's default does not matter.
 */
@ResourceLock(DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED)
public class RepeatableReadPoolTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  private InternalStepService proxy;
  private InternalStepServiceImpl impl;

  @BeforeEach
  void beforeEach() {
    var hikari = new HikariConfig();
    hikari.setJdbcUrl(pgContainer.jdbcUrl());
    hikari.setUsername(pgContainer.username());
    hikari.setPassword(pgContainer.password());
    hikari.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
    dataSource = new HikariDataSource(hikari);

    dbos = new DBOS(DBOSConfig.defaults("repeatable-read-pool").withDataSource(dataSource));
    impl = new InternalStepServiceImpl(dbos);
    proxy = dbos.registerProxy(InternalStepService.class, impl);
    impl.self = proxy;
    dbos.launch();
  }

  @AfterEach
  void afterEach() {
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED);
    impl.release.countDown();
  }

  @Test
  public void aChildWhoseInsertHoldsItsParentIsCancelledWithTheTree() throws Exception {
    var parentId = "repeatable-read-%d".formatted(System.nanoTime());
    var childId = parentId + "-child";
    var holding = new CountDownLatch(1);
    var releaseInsert = new CountDownLatch(1);
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (parentId.equals(DBOSContextHolder.get().getWorkflowId())) {
                    holding.countDown();
                    try {
                      releaseInsert.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                  }
                }));

    dbos.startWorkflow(() -> proxy.startsChild(childId), new StartWorkflowOptions(parentId));
    assertTrue(holding.await(10, TimeUnit.SECONDS), "the child's insert holds the parent");

    var cancel = CompletableFuture.runAsync(() -> dbos.cancelWorkflow(parentId, true));
    Thread.sleep(500);
    assertFalse(cancel.isDone(), "the cancel waits on the parent's row");
    releaseInsert.countDown();
    cancel.get(30, TimeUnit.SECONDS);

    assertEquals(WorkflowState.CANCELLED, statusOf(parentId));
    assertEquals(WorkflowState.CANCELLED, statusOf(childId), "the new child did not escape");
  }

  private WorkflowState statusOf(String workflowId) {
    return dbos.retrieveWorkflow(workflowId).getStatus().status();
  }
}
