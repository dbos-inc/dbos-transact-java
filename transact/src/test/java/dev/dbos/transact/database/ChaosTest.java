package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.jupiter.api.AutoClose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

// TODO: finish this test, run it many times but only in cloud
// Tracking issue: https://github.com/dbos-inc/dbos-transact-java/issues/319
// @org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class ChaosTest {
  private static final Logger logger = LoggerFactory.getLogger(ChaosTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  // @Test
  public void chaosTest() throws Exception {
    var dbosConfig = pgContainer.dbosConfig();
    try (var dataSource = pgContainer.dataSource();
        var dbos = new DBOS(dbosConfig)) {

      var impl = new ChaosServiceImpl(dbos, dataSource);
      var proxy = dbos.registerProxy(ChaosService.class, impl);
      impl.setSelf(proxy);

      dbos.launch();
      DBOSTestAccess.getSystemDatabase(dbos).speedUpPollingForTest();

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
