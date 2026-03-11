package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WorkflowClassName("TheImplFormerlyNamedSimpleServiceImpl")
public class SimpleServiceImpl implements SimpleService {

  private static final Logger logger = LoggerFactory.getLogger(SimpleServiceImpl.class);

  private final DBOS.Instance dbos;

  private SimpleService simpleService;

  public int executionCount = 0;

  public SimpleServiceImpl(DBOS.Instance dbos) {
    this.dbos = dbos;
  }

  @Override
  public void setSimpleService(SimpleService service) {
    this.simpleService = service;
  }

  @Override
  @Workflow(name = "workWithString")
  public String workWithString(String input) {
    logger.info("Executed workflow workWithString");
    executionCount++;
    return "Processed: " + input;
  }

  @Override
  @Workflow(name = "workError")
  public void workWithError() throws Exception {
    throw new Exception("DBOS Test error");
  }

  @Override
  @Workflow(name = "parentWorkflowWithoutSet")
  public String parentWorkflowWithoutSet(String input) {
    String result = input;

    result = result + simpleService.childWorkflow("abc");

    return result;
  }

  @Override
  @Workflow(name = "childWorkflow")
  public String childWorkflow(String input) {
    return input;
  }

  @Override
  @Workflow(name = "workflowWithMultipleChildren")
  public String workflowWithMultipleChildren(String input) throws Exception {
    String result = input;

    try (var id = new WorkflowOptions("child1").setContext()) {
      simpleService.childWorkflow("abc");
    }
    result = result + dbos.retrieveWorkflow("child1").getResult();

    try (var id = new WorkflowOptions("child2").setContext()) {
      simpleService.childWorkflow2("def");
    }
    result = result + dbos.retrieveWorkflow("child2").getResult();

    try (var id = new WorkflowOptions("child3").setContext()) {
      simpleService.childWorkflow3("ghi");
    }
    result = result + dbos.retrieveWorkflow("child3").getResult();

    return result;
  }

  @Override
  @Workflow(name = "childWorkflow2")
  public String childWorkflow2(String input) {
    return input;
  }

  @Override
  @Workflow(name = "childWorkflow3")
  public String childWorkflow3(String input) {
    return input;
  }

  @Override
  @Workflow(name = "childWorkflow4")
  public String childWorkflow4(String input) throws Exception {
    String result = input;
    try (var id = new WorkflowOptions("child5").setContext()) {
      simpleService.grandchildWorkflow(input);
    }
    result = "c-" + dbos.retrieveWorkflow("child5").getResult();
    return result;
  }

  @Override
  @Workflow(name = "grandchildWorkflow")
  public String grandchildWorkflow(String input) {
    return "gc-" + input;
  }

  @Override
  @Workflow(name = "grandParent")
  public String grandParent(String input) throws Exception {
    String result = input;
    try (var id = new WorkflowOptions("child4").setContext()) {
      simpleService.childWorkflow4(input);
    }
    result = "p-" + dbos.retrieveWorkflow("child4").getResult();
    return result;
  }

  @Override
  @Workflow(name = "syncWithQueued")
  public String syncWithQueued() {

    logger.info("In syncWithQueued {}", DBOS.workflowId());
    var childQ = dbos.getQueue("childQ").get();

    for (int i = 0; i < 3; i++) {

      String wid = "child" + i;
      var options = new StartWorkflowOptions(wid).withQueue(childQ);
      dbos.startWorkflow(() -> simpleService.childWorkflow(wid), options);
    }

    return "QueuedChildren";
  }

  @Override
  @Workflow(name = "longWorkflow")
  public String longWorkflow(String input) {

    simpleService.stepWithSleep(1);
    simpleService.stepWithSleep(1);

    logger.info("Done with longWorkflow");
    return input + input;
  }

  @Override
  @Step(name = "stepWithSleep")
  public void stepWithSleep(long sleepSeconds) {

    try {
      logger.info("Step sleeping for " + sleepSeconds);
      Thread.sleep(sleepSeconds * 1000);
    } catch (Exception e) {
      logger.error("Sleep interrupted", e);
    }
  }

  @Override
  @Workflow(name = "childWorkflowWithSleep")
  public String childWorkflowWithSleep(String input, long sleepSeconds)
      throws InterruptedException {
    logger.info("Child sleeping for " + sleepSeconds);
    Thread.sleep(sleepSeconds * 1000);
    logger.info("Child done sleeping for " + sleepSeconds);
    return input;
  }

  @Override
  @Workflow(name = "longParent")
  public String longParent(String input, long sleepSeconds, long timeoutSeconds)
      throws InterruptedException {

    logger.info("In longParent");
    String workflowId = "childwf";
    var options = new StartWorkflowOptions(workflowId);
    if (timeoutSeconds > 0) {
      options = options.withTimeout(timeoutSeconds, TimeUnit.SECONDS);
    }

    var handle =
        dbos.startWorkflow(
            () -> simpleService.childWorkflowWithSleep(input, sleepSeconds), options);

    Thread.sleep(sleepSeconds * 500);

    String result = handle.getResult();

    logger.info("Done with longWorkflow");
    return input + result;
  }

  @Override
  @Workflow(name = "getResultInStep")
  public String getResultInStep(String wfid) {
    return dbos.runStep(() -> dbos.getResult(wfid), "getResInStep");
  }

  @Override
  @Workflow(name = "getStatus")
  public String getStatus(String wfid) {
    return dbos.getWorkflowStatus(wfid).status();
  }

  @Override
  @Workflow(name = "getStatusInStep")
  public String getStatusInStep(String wfid) {
    return dbos.runStep(() -> dbos.getWorkflowStatus(wfid).status(), "getStatusInStep");
  }

  @Override
  @Workflow(name = "startWfInStep")
  public void startWfInStep() {
    dbos.runStep(
        () -> {
          dbos.startWorkflow(
              () -> {
                simpleService.childWorkflow("No");
              });
        },
        "startWfInStep");
  }

  @Override
  @Workflow(name = "startWfInStepById")
  public void startWfInStepById(String childId) {
    dbos.runStep(
        () -> {
          dbos.startWorkflow(
              () -> {
                simpleService.childWorkflow("Maybe");
              },
              new StartWorkflowOptions(childId));
        },
        "startWfInStepById");
  }
}
