package dev.dbos.transact.execution;

public interface ExecutingService {

    String workflowMethod(String input) ;
    String workflowMethodWithStep(String input);
    String simpleStep(String input) ;
    void setExecutingService(ExecutingService serice) ;
}
