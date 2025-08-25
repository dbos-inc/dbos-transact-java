package dev.dbos.transact.tempworkflows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

public class InternalWorkflowsServiceImpl implements InternalWorkflowsService {

    private final DBOS dbos;

    public InternalWorkflowsServiceImpl(DBOS dbos) {
        this.dbos = dbos;
    }

    @Workflow(name = "internalSendWorkflow")
    public void sendWorkflow(String destinationId, Object message, String topic) {
        dbos.send(destinationId, message, topic);
    }
}
