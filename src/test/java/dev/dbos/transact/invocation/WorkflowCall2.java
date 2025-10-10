package dev.dbos.transact.invocation;

import java.io.Serializable;

@FunctionalInterface
public interface WorkflowCall2<A, B, R> extends Serializable {
  R call(A a, B b) throws Exception;
}
