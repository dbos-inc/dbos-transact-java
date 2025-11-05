package dev.dbos.transact.workflow;

public interface MgmtService {

  void setMgmtService(MgmtService m);

  int simpleWorkflow(int input);

  void stepOne();

  void stepTwo();

  void stepThree();

  int getStepsExecuted();
}
