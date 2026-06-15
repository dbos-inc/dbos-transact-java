package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContextHolder;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

interface MgmtService {
  int simpleWorkflow(int input) throws InterruptedException;

  int cancelAfterFinalStepWorkflow(int input) throws InterruptedException;

  void stepTimingWorkflow() throws InterruptedException;

  String helloWorkflow(String name);

  String blockingWorkflow() throws InterruptedException;

  String treeParentWorkflow(String childId, String grandchildId) throws InterruptedException;

  String treeChildWorkflow(String grandchildId) throws InterruptedException;

  String treeLeafWorkflow() throws InterruptedException;

  void selfInterruptingWorkflow() throws InterruptedException;
}

class MgmtServiceImpl implements MgmtService {

  private final DBOS dbos;
  private int stepsExecuted;
  public CountDownLatch mainLatch = new CountDownLatch(1);
  public CountDownLatch workLatch = new CountDownLatch(1);

  // Per-workflow latches for multi-workflow tests, keyed by workflow ID.
  final ConcurrentHashMap<String, CountDownLatch> testMainLatches = new ConcurrentHashMap<>();
  final ConcurrentHashMap<String, CountDownLatch> testWorkLatches = new ConcurrentHashMap<>();

  // Set from the test after registerProxy so tree workflows can start children via the proxy.
  MgmtService proxy;

  public MgmtServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  public int stepsExecuted() {
    return this.stepsExecuted;
  }

  @Override
  @Workflow(name = "myworkflow")
  public int simpleWorkflow(int input) throws InterruptedException {

    dbos.runStep(() -> ++stepsExecuted, "stepOne");
    mainLatch.countDown();
    workLatch.await();
    dbos.runStep(() -> ++stepsExecuted, "stepTwo");
    dbos.runStep(() -> ++stepsExecuted, "stepThree");

    return input;
  }

  @Override
  @Workflow
  public int cancelAfterFinalStepWorkflow(int input) throws InterruptedException {
    dbos.runStep(() -> ++stepsExecuted, "theOnlyStep");
    mainLatch.countDown();
    workLatch.await();
    return input;
  }

  @Override
  @Workflow
  public void stepTimingWorkflow() throws InterruptedException {
    for (var i = 0; i < 5; i++) {
      dbos.runStep(() -> Thread.sleep(100), "stepTimingStep");
    }

    dbos.setEvent("key", "value");
    dbos.listWorkflows(new ListWorkflowsInput());
    dbos.recv(null, Duration.ofSeconds(1)).orElse(null);
  }

  @Override
  @Workflow
  public String helloWorkflow(String name) {
    return "Hello, %s!".formatted(name);
  }

  @Override
  @Workflow
  public String blockingWorkflow() throws InterruptedException {
    var wfId = DBOSContextHolder.get().getWorkflowId();
    dbos.runStep(() -> ++stepsExecuted, "blockingStepOne");
    testMainLatches.get(wfId).countDown();
    testWorkLatches.get(wfId).await();
    dbos.runStep(() -> ++stepsExecuted, "blockingStepTwo");
    return wfId;
  }

  @Override
  @Workflow
  public String treeParentWorkflow(String childId, String grandchildId)
      throws InterruptedException {
    var wfId = DBOSContextHolder.get().getWorkflowId();
    dbos.startWorkflow(
        () -> proxy.treeChildWorkflow(grandchildId), new StartWorkflowOptions(childId));
    testMainLatches.get(wfId).countDown();
    testWorkLatches.get(wfId).await();
    dbos.runStep(() -> null, "parentStep");
    return wfId;
  }

  @Override
  @Workflow
  public String treeChildWorkflow(String grandchildId) throws InterruptedException {
    var wfId = DBOSContextHolder.get().getWorkflowId();
    dbos.startWorkflow(() -> proxy.treeLeafWorkflow(), new StartWorkflowOptions(grandchildId));
    testMainLatches.get(wfId).countDown();
    testWorkLatches.get(wfId).await();
    dbos.runStep(() -> null, "childStep");
    return wfId;
  }

  @Override
  @Workflow
  public String treeLeafWorkflow() throws InterruptedException {
    var wfId = DBOSContextHolder.get().getWorkflowId();
    testMainLatches.get(wfId).countDown();
    testWorkLatches.get(wfId).await();
    dbos.runStep(() -> null, "leafStep");
    return wfId;
  }

  @Override
  @Workflow
  public void selfInterruptingWorkflow() throws InterruptedException {
    throw new InterruptedException("simulated interruption");
  }
}
