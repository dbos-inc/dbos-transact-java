package dev.dbos.transact.workflow;

@FunctionalInterface
public interface StepShouldRetry {
  boolean shouldRetry(Throwable e);

  class None implements StepShouldRetry {
    @Override
    public boolean shouldRetry(Throwable e) {
      return true;
    }
  }
}
