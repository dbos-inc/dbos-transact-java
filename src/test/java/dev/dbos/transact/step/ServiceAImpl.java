package dev.dbos.transact.step;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.workflow.Workflow;

public class ServiceAImpl implements ServiceA {

  private ServiceB serviceBproxy;

  ServiceAImpl(ServiceB b) {
    this.serviceBproxy = b;
  }

  public void setServiceB(ServiceB serviceB) {
    serviceBproxy = serviceB;
  }

  @Workflow(name = "workflowWithSteps")
  public String workflowWithSteps(String input) {

    DBOSContext ctx = DBOSContextHolder.get();
    String wfid = ctx.getWorkflowId();

    serviceBproxy.step1("one");
    serviceBproxy.step2("two");
    try {
      serviceBproxy.step3("three", false);
    } catch (Exception e) {
      // Nothing to do
      System.out.println(e.getMessage());
    }
    serviceBproxy.step4("four");
    serviceBproxy.step5("five");

    return input + input;
  }

  @Workflow(name = "workflowWithStepsError")
  public String workflowWithStepError(String input) {

    serviceBproxy.step1("one");
    serviceBproxy.step2("two");
    try {
      serviceBproxy.step3("three", true);
    } catch (Exception e) {
      // Nothing to do
      System.out.println(e.getMessage());
    }
    serviceBproxy.step4("four");
    serviceBproxy.step5("five");

    return input + input;
  }
}
