package dev.dbos.transact.config;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

interface ExecutorTestService {
  Integer workflow();
}

class ExecutorTestServiceImpl implements ExecutorTestService {

  private final DBOS dbos;

  public ExecutorTestServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public Integer workflow() {
    var a = dbos.runStep(() -> 1, "stepOne");
    var b = dbos.runStep(() -> 2, "stepTwo");
    var c = dbos.runStep(() -> 3, "stepThree");
    return a + b + c;
  }
}
