package dev.dbos.transact.workflow;

public interface GCService {

    int testStep(int x);

    int testWorkflow(int x);

    String gcBlockedWorkflow();

    String timeoutBlockedWorkflow();

    void setGCService(GCService service);
}
