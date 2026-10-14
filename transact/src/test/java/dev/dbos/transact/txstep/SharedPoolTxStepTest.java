package dev.dbos.transact.txstep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;

interface SharedPoolService {
  String holdConnection() throws Exception;

  String shortSteps() throws Exception;
}

class SharedPoolServiceImpl implements SharedPoolService {
  private final JdbcStepFactory stepFactory;
  private final CyclicBarrier allHolding;

  SharedPoolServiceImpl(JdbcStepFactory stepFactory, CyclicBarrier allHolding) {
    this.stepFactory = stepFactory;
    this.allHolding = allHolding;
  }

  @Override
  @Workflow
  public String holdConnection() throws Exception {
    return stepFactory.txStep(
        conn -> {
          conn.createStatement().execute("SELECT 1");
          // Every connection in the pool is now held by a step's transaction.
          allHolding.await(30, TimeUnit.SECONDS);
          return "committed";
        },
        "holdConnection");
  }

  @Override
  @Workflow
  public String shortSteps() throws Exception {
    for (int i = 0; i < 3; i++) {
      stepFactory.txStep(
          (Connection conn) -> conn.createStatement().execute("SELECT pg_sleep(0.02)"),
          "short" + i);
    }
    return "committed";
  }
}

/**
 * Steps on the pool the system database also uses check ownership on their own connection. Taking a
 * second connection for it would leave steps that together hold the whole pool each waiting for one
 * that never frees. A factory that reports the shared pool is recognized by it; one that cannot is
 * recognized by its connection reaching the system database.
 */
class SharedPoolTxStepTest {

  static final int POOL_SIZE = 3;

  @AutoClose final PgContainer pgContainer = new PgContainer();

  // A JDBC factory that cannot say which pool it uses, as a Jdbi factory created without one.
  private static JdbcStepFactory hidingItsPool(DBOS dbos, DataSource pool) {
    return new JdbcStepFactory(dbos, pool) {
      @Override
      protected DataSource dataSource() {
        return null;
      }
    };
  }

  @Test
  void stepsHoldingEveryConnectionOfTheSharedPoolStillCommit() throws Exception {
    // The factory names its pool, so no connection needs probing.
    assertEquals(0, runHoldingThePool(JdbcStepFactory::new));
  }

  @Test
  void aFactoryThatCannotReportTheSharedPoolStillCommits() throws Exception {
    Assumptions.assumeFalse(
        PgContainer.USE_COCKROACH_DB,
        "PG-only: CockroachDB cannot identify its database, so steps there take a second connection");
    var probed = runHoldingThePool(SharedPoolTxStepTest::hidingItsPool);
    assertTrue(probed >= 1 && probed <= POOL_SIZE, "probed " + probed + " times");
  }

  @Test
  void aFactoryThatCannotReportTheSharedPoolKeepsUpUnderLoad() throws Exception {
    Assumptions.assumeFalse(
        PgContainer.USE_COCKROACH_DB,
        "PG-only: CockroachDB cannot identify its database, so steps there take a second connection");
    var probed =
        withSharedPool(
            SharedPoolTxStepTest::hidingItsPool,
            (dbos, proxy) -> {
              var handles = new ArrayList<WorkflowHandle<String, Exception>>();
              for (int i = 0; i < 30; i++) {
                handles.add(
                    dbos.startWorkflow(
                        () -> proxy.shortSteps(),
                        new StartWorkflowOptions("shared-pool-load-" + i)));
              }
              return handles;
            });
    // 90 steps, but each physical connection is probed once.
    assertTrue(probed >= 1 && probed <= POOL_SIZE, "probed " + probed + " times");
  }

  // POOL_SIZE workflows whose steps each hold a connection until all of them do. Returns how many
  // times a connection was probed.
  private int runHoldingThePool(BiFunction<DBOS, DataSource, JdbcStepFactory> newFactory)
      throws Exception {
    return withSharedPool(
        newFactory,
        (dbos, proxy) -> {
          var handles = new ArrayList<WorkflowHandle<String, Exception>>();
          for (int i = 0; i < POOL_SIZE; i++) {
            handles.add(
                dbos.startWorkflow(
                    () -> proxy.holdConnection(), new StartWorkflowOptions("shared-pool-" + i)));
          }
          return handles;
        });
  }

  @FunctionalInterface
  private interface Starter {
    ArrayList<WorkflowHandle<String, Exception>> start(DBOS dbos, SharedPoolService proxy)
        throws Exception;
  }

  // Runs the workflows the starter starts on a pool DBOS shares, and returns how many times the
  // same-database probe checked a connection.
  private int withSharedPool(
      BiFunction<DBOS, DataSource, JdbcStepFactory> newFactory, Starter starter) throws Exception {
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
        var impl =
            new SharedPoolServiceImpl(newFactory.apply(dbos, pool), new CyclicBarrier(POOL_SIZE));
        var proxy = dbos.registerProxy(SharedPoolService.class, impl);
        dbos.launch();

        var handles = starter.start(dbos, proxy);
        assertTimeoutPreemptively(
            Duration.ofSeconds(60),
            () -> {
              for (var handle : handles) {
                assertEquals("committed", handle.getResult());
              }
            });
        return DBTestAccess.sameDatabaseProbes(DBOSTestAccess.getSystemDatabase(dbos));
      }
    }
  }
}
