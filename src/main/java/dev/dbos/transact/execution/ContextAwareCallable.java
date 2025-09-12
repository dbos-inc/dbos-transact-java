package dev.dbos.transact.execution;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextAwareCallable<T> implements Callable<T> {
  private final Callable<T> task;
  private DBOSContext capturedContext;

  Logger logger = LoggerFactory.getLogger(ContextAwareCallable.class);

  public ContextAwareCallable(DBOSContext ctx, Callable<T> task) {
    this.task = task;
    this.capturedContext = ctx;
  }

  @Override
  public T call() throws Exception {
    DBOSContextHolder.set(capturedContext);
    try {
      return task.call();
    } finally {
      DBOSContextHolder.clear();
    }
  }
}
