package dev.dbos.transact.workflow;

public interface SimpleService {

    public WorkflowHandle<String> workWithString(String input);

    public WorkflowHandle workWithError() throws Exception ;
}
