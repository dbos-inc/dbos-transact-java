package dev.dbos.transact.step;

public interface ServiceWFAndStep {

    void setSelf(ServiceWFAndStep serviceWFAndStep);

    String aWorkflow(String input);

    String stepOne(String input);

    String stepTwo(String input);

    String aWorkflowWithInlineSteps(String input);
}
