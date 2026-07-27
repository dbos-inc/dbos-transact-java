package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

interface ChaosService {

  String dbLossBetweenSteps();

  String runChildWf();

  String wfPart1();

  String wfPart2(String id1);
}

class ChaosServiceImpl implements ChaosService {

  private final DBOS dbos;
  private final DataSource dataSource;

  private ChaosService self;

  public ChaosServiceImpl(DBOS dbos, DataSource dataSource) {
    this.dbos = dbos;
    this.dataSource = dataSource;
  }

  public void setSelf(ChaosService self) {
    this.self = self;
  }

  @Override
  @Workflow
  public String dbLossBetweenSteps() {
    dbos.runStep(() -> "A", "A");
    dbos.runStep(() -> "B", "B");
    causeChaos(dataSource);
    dbos.runStep(() -> "C", "C");
    dbos.runStep(() -> "D", "D");
    return "Hehehe";
  }

  @Override
  @Workflow
  public String runChildWf() {
    causeChaos(dataSource);
    var handle = dbos.startWorkflow(() -> self.dbLossBetweenSteps());
    causeChaos(dataSource);
    return handle.getResult();
  }

  @Override
  @Workflow
  public String wfPart1() {
    // causeChaos(dataSource);
    var r = dbos.<String>recv("topic", Duration.ofSeconds(5)).orElseThrow();
    // causeChaos(dataSource);
    dbos.setEvent("key", "v1");
    // causeChaos(dataSource);
    return "Part1" + r;
  }

  @Override
  @Workflow
  public String wfPart2(String id1) {
    // causeChaos(dataSource);
    dbos.send(id1, "hello1", "topic");
    // causeChaos(dataSource);
    var v1 = dbos.<String>getEvent(id1, "key", Duration.ofSeconds(5)).orElseThrow();
    // causeChaos(dataSource);
    return "Part2" + v1;
  }

  static void causeChaos(DataSource ds) {
    try (var conn = ds.getConnection();
        var st = conn.createStatement()) {

      st.execute(
          """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND datname = current_database();
        """);
    } catch (SQLException e) {
      throw new RuntimeException("Could not cause chaos, credentials insufficient?", e);
    }
  }
}

@Tag("ci-only")
public class ChaosTest {
  private static final Logger logger = LoggerFactory.getLogger(ChaosTest.class);
  private static final int REPETITIONS = 5;

  @RepeatedTest(REPETITIONS)
  public void chaosTest() throws Exception {
    assumeFalse(
        PgContainer.USE_COCKROACH_DB, "pg_terminate_backend() not supported on CockroachDB");
    try (var pgContainer = new PgContainer()) {
      var dbosConfig = pgContainer.dbosConfig();
      try (var dataSource = pgContainer.dataSource();
          var dbos = new DBOS(dbosConfig)) {

        var impl = new ChaosServiceImpl(dbos, dataSource);
        var proxy = dbos.registerProxy(ChaosService.class, impl);
        impl.setSelf(proxy);

        dbos.launch();

        assertEquals("Hehehe", proxy.dbLossBetweenSteps());

        assertEquals("Hehehe", proxy.runChildWf());

        var h1 = dbos.startWorkflow(() -> proxy.wfPart1());
        var h2 = dbos.startWorkflow(() -> proxy.wfPart2(h1.workflowId()));

        if (!"Part1hello1".equals(h1.getResult()) || !"Part2v1".equals(h2.getResult())) {
          logWorkflowDetails(dataSource, "Part 1", h1.workflowId());
          logWorkflowDetails(dataSource, "Part 2", h2.workflowId());
        }

        assertEquals("Part1hello1", h1.getResult());
        assertEquals("Part2v1", h2.getResult());
      }
    }
  }

  @RepeatedTest(REPETITIONS)
  public void toxiProxyTest() throws Exception {
    try (var network = Network.newNetwork();
        var pg = PgContainer.getPG()) {

      pg.withNetwork(network).withNetworkAliases("postgres");
      pg.start();

      var directUrl = pg.getJdbcUrl().replaceFirst("/[^/]+$", "/dbos_test_db");
      MigrationManager.runMigrations(directUrl, pg.getUsername(), pg.getPassword(), "dbos", true);

      try (var toxiContainer = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0")) {
        toxiContainer.withNetwork(network);
        toxiContainer.start();

        var toxiClient =
            new ToxiproxyClient(toxiContainer.getHost(), toxiContainer.getControlPort());
        var proxy = toxiClient.createProxy("pg", "0.0.0.0:8666", "postgres:5432");
        var proxiedUrl =
            "jdbc:postgresql://%s:%d/dbos_test_db"
                .formatted(toxiContainer.getHost(), toxiContainer.getMappedPort(8666));

        var dbosConfig =
            DBOSConfig.defaults("chaos-toxi-test")
                .withDatabaseUrl(proxiedUrl)
                .withDbUser(pg.getUsername())
                .withDbPassword(pg.getPassword());

        try (var directDs =
                dev.dbos.transact.database.SystemDatabase.createDataSource(
                    directUrl, pg.getUsername(), pg.getPassword());
            var dbos = new DBOS(dbosConfig)) {

          var impl = new ChaosServiceImpl(dbos, directDs);
          var svc = dbos.registerProxy(ChaosService.class, impl);
          impl.setSelf(svc);
          dbos.launch();

          // Scenario 1: proxy disabled — simulates a sustained network partition
          proxy.disable();
          // Dedicated thread: this blocks in dbRetry while the proxy is down, and must
          // not pin a common ForkJoinPool worker (starves unrelated tests' callbacks).
          var wf1 =
              CompletableFuture.supplyAsync(
                  () -> dbos.startWorkflow(() -> svc.dbLossBetweenSteps()),
                  r -> new Thread(r, "chaos-wf1").start());
          Thread.sleep(3000);
          proxy.enable();
          assertEquals("Hehehe", wf1.get(10, TimeUnit.SECONDS).getResult());

          // Scenario 2: TCP RST mid-execution — simulates a flapping connection
          var wf2 = dbos.startWorkflow(() -> svc.dbLossBetweenSteps());
          proxy.toxics().resetPeer("reset", ToxicDirection.DOWNSTREAM, 0);
          Thread.sleep(3000);
          proxy.toxics().get("reset").remove();
          assertEquals("Hehehe", wf2.getResult());
        }
      }
    }
  }

  void logWorkflowDetails(DataSource dataSource, String name, String wfid) throws Exception {
    var wfstat = DBUtils.getWorkflowRow(dataSource, wfid);
    logger.info("Workflow ({}) ID: {}. Status {}", name, wfid, wfstat.status());

    var steps = DBUtils.getStepRows(dataSource, wfid);
    for (var step : steps) {
      logger.info("  - # {} {} {}", step.functionId(), step.functionName(), step.output());
    }

    var events = DBUtils.getWorkflowEvents(dataSource, wfid);
    for (var event : events) {
      logger.info("  $ {}", event.toString());
    }
  }
}
