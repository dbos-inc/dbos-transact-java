package dev.dbos.transact.jdbi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;

interface JdbiSharedPoolService {
  String holdConnection() throws Exception;
}

class JdbiSharedPoolServiceImpl implements JdbiSharedPoolService {
  private final JdbiStepFactory stepFactory;
  private final CyclicBarrier allHolding;

  JdbiSharedPoolServiceImpl(JdbiStepFactory stepFactory, CyclicBarrier allHolding) {
    this.stepFactory = stepFactory;
    this.allHolding = allHolding;
  }

  @Override
  @Workflow
  public String holdConnection() throws Exception {
    return stepFactory.inStep(
        handle -> {
          handle.execute("SELECT 1");
          // Every connection in the pool is now held by a step's transaction.
          allHolding.await(30, TimeUnit.SECONDS);
          return "committed";
        },
        "holdConnection");
  }
}

/**
 * A Jdbi step factory given the system database's pool checks ownership on the step's own
 * connection. Taking a second connection for it would leave steps that together hold the whole pool
 * each waiting for one that never frees.
 */
class JdbiSharedPoolTxStepTest {

  static final int POOL_SIZE = 3;

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @Test
  void stepsHoldingEveryConnectionOfTheSharedPoolStillCommit() throws Exception {
    var hikari = new HikariConfig();
    hikari.setJdbcUrl(pgContainer.jdbcUrl());
    hikari.setUsername(pgContainer.username());
    hikari.setPassword(pgContainer.password());
    hikari.setMaximumPoolSize(POOL_SIZE);
    hikari.setConnectionTimeout(5_000);

    try (var pool = new HikariDataSource(hikari)) {
      // LISTEN would hold a pool connection of its own.
      var config = pgContainer.dbosConfig().withDataSource(pool).withUseListenNotify(false);
      try (var dbos = new DBOS(config)) {
        var factory = new JdbiStepFactory(dbos, Jdbi.create(pool), pool);
        var impl = new JdbiSharedPoolServiceImpl(factory, new CyclicBarrier(POOL_SIZE));
        var proxy = dbos.registerProxy(JdbiSharedPoolService.class, impl);
        dbos.launch();

        var handles = new ArrayList<WorkflowHandle<String, Exception>>();
        for (int i = 0; i < POOL_SIZE; i++) {
          handles.add(
              dbos.startWorkflow(
                  () -> proxy.holdConnection(), new StartWorkflowOptions("jdbi-shared-pool-" + i)));
        }

        assertTimeoutPreemptively(
            Duration.ofSeconds(60),
            () -> {
              for (var handle : handles) {
                assertEquals("committed", handle.getResult());
              }
            });
      }
    }
  }
}
