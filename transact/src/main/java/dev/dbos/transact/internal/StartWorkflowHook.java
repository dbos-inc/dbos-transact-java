package dev.dbos.transact.internal;

@FunctionalInterface
public interface StartWorkflowHook {

  void invoke(Invocation invocation);
}
