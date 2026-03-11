package dev.dbos.transact.step;

import dev.dbos.transact.workflow.Workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceAImpl implements ServiceA {

  private static final Logger logger = LoggerFactory.getLogger(ServiceAImpl.class);

  private final ServiceB serviceBproxy;

  ServiceAImpl(ServiceB b) {
    this.serviceBproxy = b;
  }

  @Override
  @Workflow(name = "workflowWithSteps")
  public String workflowWithSteps(String input) {
    serviceBproxy.step1("one");
    serviceBproxy.step2("two");
    try {
      serviceBproxy.step3("three", false);
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
    serviceBproxy.step4("four");
    serviceBproxy.step5("five");

    return input + input;
  }

  @Override
  @Workflow(name = "workflowWithStepsError")
  public String workflowWithStepError(String input) {
    serviceBproxy.step1("one");
    serviceBproxy.step2("two");
    try {
      serviceBproxy.step3("three", true);
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
    serviceBproxy.step4("four");
    serviceBproxy.step5("five");

    return input + input;
  }
}
