package dev.dbos.transact.database;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import javax.sql.DataSource;

public class DisruptiveServiceImpl implements DisruptiveService {

  private DisruptiveService self;
  private DataSource ds;

  public void setSelf(DisruptiveService service) {
    this.self = service;
  }

  public void setDS(DataSource ds) {
    this.ds = ds;
  }

  @Override
  @Workflow()
  public String dbLossBetweenSteps() {
    DBOS.runStep(
        () -> {
          return "A";
        },
        "A");
    DBOS.runStep(
        () -> {
          return "B";
        },
        "B");
    causeChaos(ds);
    DBOS.runStep(
        () -> {
          return "C";
        },
        "C");
    DBOS.runStep(
        () -> {
          return "D";
        },
        "D");
    return "Hehehe";
  }

  @Override
  @Workflow()
  public String runChildWf() {
    causeChaos(ds);
    var wfh = DBOS.startWorkflow(() -> self.dbLossBetweenSteps());
    causeChaos(ds);
    return wfh.getResult();
  }

  @Override
  @Workflow()
  public String wfPart1() {
    causeChaos(ds);
    var r = (String) DBOS.recv("topic", Duration.ofSeconds(5));
    causeChaos(ds);
    DBOS.setEvent("key", "v1");
    causeChaos(ds);
    return "Part1" + r;
  }

  @Override
  @Workflow()
  public String wfPart2(String id1) {
    causeChaos(ds);
    DBOS.send(id1, "hello1", "topic");
    causeChaos(ds);
    var v1 = (String) DBOS.getEvent(id1, "key", Duration.ofSeconds(5));
    causeChaos(ds);
    return "Part2" + v1;
  }

  static void causeChaos(DataSource ds) {
    try (Connection conn = ds.getConnection();
        Statement st = conn.createStatement()) {

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
