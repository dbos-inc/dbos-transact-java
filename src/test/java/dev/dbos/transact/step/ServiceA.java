package dev.dbos.transact.step;

public interface ServiceA {

    public void setServiceB(ServiceB serviceB);

    public String workflowWithSteps(String input);

    public String workflowWithStepError(String input);
}
