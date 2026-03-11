package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutingServiceImpl implements ExecutingService {

  private static final Logger logger = LoggerFactory.getLogger(ExecutingServiceImpl.class);

  private final DBOS.Instance dbos;
  private ExecutingService self;
  public int step1Count = 0;
  public int step2Count = 0;

  public ExecutingServiceImpl(DBOS.Instance dbos) {
    this.dbos = dbos;
  }

  public void setSelf(ExecutingService self) {
    this.self = self;
  }

  @Override
  @Workflow(name = "workflowMethod")
  public String workflowMethod(String input) {
    return input + input;
  }

  @Override
  @Workflow(name = "workflowMethodWithStep")
  public String workflowMethodWithStep(String input) {
    String step1Response = self.stepOne("stepOne");
    String step2Response = self.stepTwo("stepTwo");
    return input + step1Response + step2Response;
  }

  @Override
  @Step(name = "stepOne")
  public String stepOne(String input) {
    ++step1Count;
    return input;
  }

  @Override
  @Step(name = "stepTwo")
  public String stepTwo(String input) {
    ++step2Count;
    return input;
  }

  @Override
  @Workflow(name = "sleepingWorkflow")
  public void sleepingWorkflow(double seconds) {
    dbos.sleep(Duration.ofMillis((long) (seconds * 1000)));
  }

  public int callsToNoReturnStep = 0;

  @Override
  @Step(name = "noReturn")
  public void stepWithNoReturn() {
    ++callsToNoReturnStep;
    return;
  }

  public int callsToThrowStep = 0;

  @Override
  @Step(name = "throws", retriesAllowed = false)
  public void stepThatThrows() throws MyAppException {
    ++callsToThrowStep;
    throw new MyAppException();
  }

  @Override
  @Workflow(name = "workflowWithNoResults")
  public void workflowWithNoResultSteps() {
    try {
      self.stepThatThrows();
    } catch (MyAppException e) {
      self.stepWithNoReturn();
    } catch (Exception e) {
      logger.error("Caught an unexpected exception of type: {}", e.getClass().getName(), e);
    }
  }
}
