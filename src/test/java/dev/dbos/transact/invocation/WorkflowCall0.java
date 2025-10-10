package dev.dbos.transact.invocation;

import java.io.Serializable;

@FunctionalInterface
public interface WorkflowCall0<R> extends Serializable {
  R call() throws Exception;
}
