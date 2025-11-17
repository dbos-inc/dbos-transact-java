package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;

public class ForkServiceImpl implements ForkService {

  private ForkService forkService;

  public int step1Count;
  public int step2Count;
  public int step3Count;
  public int step4Count;
  public int step5Count;
  public int child1Count;
  public int child2Count;

  public void setForkService(ForkService s) {
    this.forkService = s;
  }

  @Workflow(name = "worfklow")
  public String simpleWorkflow(String input) {
    forkService.stepOne("one");
    forkService.stepTwo(2);
    forkService.stepThree(2.5f);
    forkService.stepFour(Double.valueOf(23.73));
    forkService.stepFive(false);

    return input + input;
  }

  @Workflow(name = "parent")
  public String parentChild(String input) {

    forkService.stepOne("one");
    forkService.stepTwo(2);

    try (var o = new WorkflowOptions("child1").setContext()) {
      forkService.child1(25);
    }

    try (var o = new WorkflowOptions("child2").setContext()) {
      forkService.child2(25.75f);
    }

    forkService.stepFive(false);
    return input + input;
  }

  @Workflow(name = "parentasync")
  public String parentChildAsync(String input) {
    forkService.stepOne("one");
    forkService.stepTwo(2);

    DBOS.startWorkflow(() -> forkService.child1(25), new StartWorkflowOptions("child1"));
    DBOS.startWorkflow(() -> forkService.child2(25.75f), new StartWorkflowOptions("child2"));

    forkService.stepFive(false);
    return input + input;
  }

  @Step(name = "one")
  public String stepOne(String input) {
    ++step1Count;
    return input;
  }

  @Step(name = "two")
  public int stepTwo(Integer input) {
    ++step2Count;
    return input;
  }

  @Step(name = "three")
  public float stepThree(Float input) {
    ++step3Count;
    return input;
  }

  @Step(name = "four")
  public double stepFour(Double input) {
    ++step4Count;
    return input;
  }

  @Step(name = "five")
  public void stepFive(boolean b) {
    ++step5Count;
  }

  @Workflow
  public String child1(Integer number) {
    ++child1Count;
    return String.valueOf(number);
  }

  @Workflow
  public String child2(Float number) {
    ++child2Count;
    return String.valueOf(number);
  }

  @Workflow
  public void setEventWorkflow(String key) throws InterruptedException {
    DBOS.setEvent(key, "event-%d".formatted(System.currentTimeMillis()));
    Thread.sleep(100);
    DBOS.setEvent(key, "event-%d".formatted(System.currentTimeMillis()));
    Thread.sleep(100);
    DBOS.setEvent(key, "event-%d".formatted(System.currentTimeMillis()));
    Thread.sleep(100);
    DBOS.setEvent(key, "event-%d".formatted(System.currentTimeMillis()));
    Thread.sleep(100);
    DBOS.setEvent(key, "event-%d".formatted(System.currentTimeMillis()));
  }
}
