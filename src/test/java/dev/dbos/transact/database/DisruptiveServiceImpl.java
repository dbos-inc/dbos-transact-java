package dev.dbos.transact.database;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptiveServiceImpl implements DisruptiveService {

  private static final Logger logger = LoggerFactory.getLogger(DisruptiveServiceImpl.class);

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
    DBUtils.causeChaos(ds);
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
    DBUtils.causeChaos(ds);
    var wfh = DBOS.startWorkflow(() -> self.dbLossBetweenSteps());
    DBUtils.causeChaos(ds);
    return wfh.getResult();
  }

  @Override
  @Workflow()
  public String wfPart1() {
    DBUtils.causeChaos(ds);
    var r = (String) DBOS.recv("topic", Duration.ofSeconds(5));
    DBUtils.causeChaos(ds);
    DBOS.setEvent("key", "v1");
    DBUtils.causeChaos(ds);
    return "Part1" + r;
  }

  @Override
  @Workflow()
  public String wfPart2(String id1) {
    DBUtils.causeChaos(ds);
    DBOS.send(id1, "hello1", "topic");
    DBUtils.causeChaos(ds);
    var v1 = (String) DBOS.getEvent(id1, "key", Duration.ofSeconds(5));
    DBUtils.causeChaos(ds);
    return "Part2" + v1;
  }
}
