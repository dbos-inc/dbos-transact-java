package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface SimpleService {

  public String workWithString(String input);

  public void workWithError() throws Exception;

  public String parentWorkflowWithoutSet(String input);

  public String workflowWithMultipleChildren(String input) throws Exception;

  public String childWorkflow(String input);

  public String childWorkflow2(String input);

  public String childWorkflow3(String input);

  public String childWorkflow4(String input) throws Exception;

  public String grandchildWorkflow(String input);

  public String grandParent(String input) throws Exception;

  String syncWithQueued();

  String longWorkflow(String input);

  void stepWithSleep(long sleepSeconds);

  String longParent(String input, long sleepSeconds, long timeoutSeconds)
      throws InterruptedException;

  String childWorkflowWithSleep(String input, long sleepSeconds) throws InterruptedException;

  String getResultInStep(String wfid);

  String getStatus(String wfid);

  String getStatusInStep(String wfid);

  void startWfInStep();

  void startWfInStepById(String childId);
}

@WorkflowClassName("TheImplFormerlyNamedSimpleServiceImpl")
class SimpleServiceImpl implements SimpleService {

  private static final Logger logger = LoggerFactory.getLogger(SimpleServiceImpl.class);

  private final DBOS dbos;

  private SimpleService self;

  public int executionCount = 0;

  public SimpleServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  public void setSelf(SimpleService self) {
    this.self = self;
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

    result = result + self.childWorkflow("abc");

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
      self.childWorkflow("abc");
    }
    result = result + dbos.retrieveWorkflow("child1").getResult();

    try (var id = new WorkflowOptions("child2").setContext()) {
      self.childWorkflow2("def");
    }
    result = result + dbos.retrieveWorkflow("child2").getResult();

    try (var id = new WorkflowOptions("child3").setContext()) {
      self.childWorkflow3("ghi");
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
      self.grandchildWorkflow(input);
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
      self.childWorkflow4(input);
    }
    result = "p-" + dbos.retrieveWorkflow("child4").getResult();
    return result;
  }

  @Override
  @Workflow(name = "syncWithQueued")
  public String syncWithQueued() {

    logger.info("In syncWithQueued {}", DBOS.workflowId());
    var childQ = dbos.findQueue("childQ").get();

    for (int i = 0; i < 3; i++) {

      String wid = "child" + i;
      var options = new StartWorkflowOptions(wid).withQueue(childQ);
      dbos.startWorkflow(() -> self.childWorkflow(wid), options);
    }

    return "QueuedChildren";
  }

  @Override
  @Workflow(name = "longWorkflow")
  public String longWorkflow(String input) {

    self.stepWithSleep(1);
    self.stepWithSleep(1);

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
        dbos.startWorkflow(() -> self.childWorkflowWithSleep(input, sleepSeconds), options);

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
    return dbos.getWorkflowStatus(wfid).orElseThrow().status().name();
  }

  @Override
  @Workflow(name = "getStatusInStep")
  public String getStatusInStep(String wfid) {
    return dbos.runStep(
        () -> dbos.getWorkflowStatus(wfid).orElseThrow().status().name(), "getStatusInStep");
  }

  @Override
  @Workflow(name = "startWfInStep")
  public void startWfInStep() {
    dbos.runStep(
        () -> {
          dbos.startWorkflow(
              () -> {
                self.childWorkflow("No");
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
                self.childWorkflow("Maybe");
              },
              new StartWorkflowOptions(childId));
        },
        "startWfInStepById");
  }
}
