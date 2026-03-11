package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;

interface ForkTestService {

  String simpleWorkflow(String input);

  String parentChild(String input);

  String parentChildAsync(String input);

  String stepOne(String input);

  int stepTwo(Integer input);

  float stepThree(Float input);

  double stepFour(Double input);

  void stepFive(boolean b);

  String child1(Integer number);

  String child2(Float number);

  void setEventWorkflow(String key) throws InterruptedException;
}

class ForkTestServiceImpl implements ForkTestService {

  private final DBOS.Instance dbos;
  private ForkTestService proxy;

  public int step1Count;
  public int step2Count;
  public int step3Count;
  public int step4Count;
  public int step5Count;
  public int child1Count;
  public int child2Count;

  public ForkTestServiceImpl(DBOS.Instance dbos) {
    this.dbos = dbos;
  }

  public void setProxy(ForkTestService proxy) {
    this.proxy = proxy;
  }

  @Override
  @Workflow
  public String simpleWorkflow(String input) {
    proxy.stepOne("one");
    proxy.stepTwo(2);
    proxy.stepThree(3.3f);
    proxy.stepFour(4.4);
    proxy.stepFive(false);
    return input + input;
  }

  @Override
  @Workflow
  public String parentChild(String input) {

    proxy.stepOne("one");
    proxy.stepTwo(2);
    try (var o = new WorkflowOptions("child1").setContext()) {
      proxy.child1(3);
    }
    try (var o = new WorkflowOptions("child2").setContext()) {
      proxy.child2(4.4f);
    }
    proxy.stepFive(false);
    return input + input;
  }

  @Override
  @Workflow
  public String parentChildAsync(String input) {
    proxy.stepOne("one");
    proxy.stepTwo(2);
    dbos.startWorkflow(() -> proxy.child1(25), new StartWorkflowOptions("child1"));
    dbos.startWorkflow(() -> proxy.child2(25.75f), new StartWorkflowOptions("child2"));
    proxy.stepFive(false);
    return input + input;
  }

  @Override
  @Step
  public String stepOne(String input) {
    ++step1Count;
    return input;
  }

  @Override
  @Step
  public int stepTwo(Integer input) {
    ++step2Count;
    return input;
  }

  @Override
  @Step
  public float stepThree(Float input) {
    ++step3Count;
    return input;
  }

  @Override
  @Step
  public double stepFour(Double input) {
    ++step4Count;
    return input;
  }

  @Override
  @Step
  public void stepFive(boolean b) {
    ++step5Count;
  }

  @Override
  @Workflow
  public String child1(Integer number) {
    ++child1Count;
    return String.valueOf(number);
  }

  @Override
  @Workflow
  public String child2(Float number) {
    ++child2Count;
    return String.valueOf(number);
  }

  @Override
  @Workflow
  public void setEventWorkflow(String key) throws InterruptedException {
    for (int i = 1; i <= 5; i++) {
      dbos.setEvent(key, "event-%d".formatted(i));
      Thread.sleep(100);
    }
  }
}
