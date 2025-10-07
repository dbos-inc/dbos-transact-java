package dev.dbos.transact.step;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.Workflow;

public class ServiceWFAndStepImpl implements ServiceWFAndStep {

  private ServiceWFAndStep self;

  public void setSelf(ServiceWFAndStep serviceWFAndStep) {
    self = serviceWFAndStep;
  }

  @Workflow(name = "myworkflow")
  public String aWorkflow(String input) {

    String s1 = self.stepOne("one");
    String s2 = self.stepTwo("two");
    return input + s1 + s2;
  }

  @Step(name = "step1")
  public String stepOne(String input) {
    return input;
  }

  @Step(name = "step2")
  public String stepTwo(String input) {
    return input;
  }

  @Workflow(name = "aWorkflowWithInlineSteps")
  public String aWorkflowWithInlineSteps(String input) {
    var dbos = DBOSContext.dbosInstance();
    var len = dbos.runStep(() -> input.length(), new StepOptions("stringLength"));
    return (input + len);
  }

  int stepWithRetryRuns = 0;

  @Step(
      name = "stepWith2Retries",
      retriesAllowed = true,
      maxAttempts = 2,
      intervalSeconds = .01,
      backOffRate = 1)
  public String stepWith2Retries(String input) throws Exception {
    ++this.stepWithRetryRuns;
    throw new Exception("Will not ever run");
  }

  int stepWithNoRetryRuns = 0;

  @Step(name = "stepWithNoRetriesAllowed", retriesAllowed = false)
  public String stepWithNoRetriesAllowed(String input) throws Exception {
    ++stepWithNoRetryRuns;
    throw new Exception("No retries");
  }

  long startedTime = 0;
  int stepWithLongRetryRuns = 0;

  @Step(
      name = "stepWithLongRetry",
      maxAttempts = 3,
      retriesAllowed = true,
      intervalSeconds = 1,
      backOffRate = 10)
  public String stepWithLongRetry(String input) throws Exception {
    ++stepWithLongRetryRuns;
    if (startedTime == 0) {
      startedTime = System.currentTimeMillis();
      throw new Exception("First try");
    }
    if (System.currentTimeMillis() - startedTime > 500) {
      var rv = Integer.valueOf(this.stepWithLongRetryRuns).toString();
      startedTime = 0;
      return rv;
    }
    throw new Exception("Not enough time passed yet");
  }

  @Workflow(name = "retryTestWorkflow")
  public String stepRetryWorkflow(String input) {
    long ctime = System.currentTimeMillis();
    boolean caught = false;
    String result = "2 Retries: ";
    try {
      result = result + self.stepWith2Retries(input);
    } catch (Exception e) {
      caught = true;
    }
    if (!caught) {
      result += "<Step with retries should have thrown>";
    }
    if (System.currentTimeMillis() - ctime > 1000) {
      result += "<Retry took too long>";
    }
    result += this.stepWithRetryRuns;
    result += ".";

    result += "  No retry: ";
    caught = false;
    try {
      self.stepWithNoRetriesAllowed("");
    } catch (Exception e) {
      caught = true;
    }
    if (!caught) {
      result += "<Step with no retries should have thrown>";
    }
    result += this.stepWithNoRetryRuns;
    result += ".";

    ctime = System.currentTimeMillis();
    result += "  Backoff timeout: ";
    caught = false;
    try {
      self.stepWithLongRetry("");
    } catch (Exception e) {
      caught = true;
    }
    if (caught) {
      result += "<Step with long retry should have completed>";
    }
    if (System.currentTimeMillis() - ctime > 2000) {
      result += "<Step with long retry should have finished faster>";
    }
    if (System.currentTimeMillis() - ctime < 500) {
      result += "<Step with long retry should does not appear to have slept>";
    }
    result += this.stepWithLongRetryRuns;
    result += ".";

    return result;
  }
}
