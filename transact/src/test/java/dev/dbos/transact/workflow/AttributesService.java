package dev.dbos.transact.workflow;

import dev.dbos.transact.context.DBOSContext;

interface AttributesService {

  String childWorkflow();

  String parentWorkflow();

  void noopWorkflow();

  void attrWorkflow();

  int queuedWorkflow(int x);
}

class AttributesServiceImpl implements AttributesService {

  private AttributesService proxy;

  public void setProxy(AttributesService proxy) {
    this.proxy = proxy;
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
}
