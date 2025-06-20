package dev.dbos.transact.workflow;

public interface SimpleAsyncService {

    public WorkflowHandle<String> workWithString(String input);
}
