package dev.dbos.transact.step;

import dev.dbos.transact.workflow.StepShouldRetry;

public class RejectFatalExceptions implements StepShouldRetry {
  @Override
  public boolean shouldRetry(Throwable e) {
    return !(e instanceof FatalStepException);
  }
}
