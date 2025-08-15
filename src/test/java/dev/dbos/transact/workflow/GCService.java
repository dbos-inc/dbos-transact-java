package dev.dbos.transact.workflow;

public interface GCService {

    int testStep(int x);

    int testWorkflow(int x);

    String blockedWorkflow();

    void setGCService(GCService service);
}
