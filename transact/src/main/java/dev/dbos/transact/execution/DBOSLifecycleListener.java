package dev.dbos.transact.execution;

public interface DBOSLifecycleListener {
  void dbosLaunched();

  void dbosShutdown();
}
