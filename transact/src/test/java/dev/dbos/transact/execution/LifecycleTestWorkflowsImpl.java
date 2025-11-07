package dev.dbos.transact.execution;

import dev.dbos.transact.workflow.Workflow;

public class LifecycleTestWorkflowsImpl implements LifecycleTestWorkflows {
  int nWfs = 0, nInstances = 0;

  @Workflow()
  @TestLifecycleAnnotation(count = 3)
  public int runWf1(int nInstances, int nWfs) {
    this.nInstances = nInstances;
    this.nWfs = nWfs;
    return 8;
  }

  @Workflow()
  @TestLifecycleAnnotation(count = 4)
  public int runWf2(int nInstances, int nWfs) {
    return 7;
  }

  @Workflow()
  public int doNotRunWF(int nInstances, int nWfs) {
    throw new IllegalStateException();
  }
}
