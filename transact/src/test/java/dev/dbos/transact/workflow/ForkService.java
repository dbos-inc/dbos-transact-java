package dev.dbos.transact.workflow;

public interface ForkService {

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

  void setForkService(ForkService service);
}
