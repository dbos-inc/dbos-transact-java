package dev.dbos.transact.execution;

public interface LifecycleTestWorkflows {
  public int runWf1(int nClasses, int nWfs);

  public int runWf2(int nClasses, int nWfs);

  public int doNotRunWF(int nClasses, int nWfs);
}
