package dev.dbos.transact.invocation;

import java.io.Serializable;

@FunctionalInterface
public interface WorkflowCall1<A, R> extends Serializable {
  R call(A a) throws Exception;
}
