package dev.dbos.transact.execution;

public interface ExecutingService {

    String workflowMethod(String input) ;
    void setExecutingService(ExecutingService serice) ;
}
