package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.DBOSContext;

import java.util.Map;

interface AttributesService {

  String childWorkflow();

  String parentWorkflow();

  void noopWorkflow();

  void attrWorkflow();

  int queuedWorkflow(int x);

  void selfUpdateWorkflow(Map<String, Object> attributes);
}

class AttributesServiceImpl implements AttributesService {

  private AttributesService proxy;
  private DBOS dbos;

  public void setProxy(AttributesService proxy) {
    this.proxy = proxy;
  }

  public void setDbos(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String childWorkflow() {
    return DBOSContext.workflowId();
  }

  @Override
  @Workflow
  public String parentWorkflow() {
    return proxy.childWorkflow();
  }

  @Override
  @Workflow
  public void noopWorkflow() {}

  @Override
  @Workflow
  public void attrWorkflow() {}

  @Override
  @Workflow
  public int queuedWorkflow(int x) {
    return x;
  }

  // Updates its own attributes from within the workflow; the update is recorded as a step.
  @Override
  @Workflow
  public void selfUpdateWorkflow(Map<String, Object> attributes) {
    dbos.updateWorkflowAttributes(DBOSContext.workflowId(), attributes);
  }
}
