package dev.dbos.transact.config;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

interface AltVersionService {
  String noop();
}

class AltVersionServiceImpl implements AltVersionService {

  private final DBOS dbos;

  public AltVersionServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String noop() {
    return dbos.runStep(() -> "noop", "noopStep");
  }
}
