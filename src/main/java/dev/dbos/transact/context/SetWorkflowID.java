package dev.dbos.transact.context;

public class SetWorkflowID implements AutoCloseable {
    private final String previousWorkflowId;

    public SetWorkflowID(String workflowId) {
        DBOSContext context = DBOSContextHolder.get();
        this.previousWorkflowId = context.getWorkflowId();
        context.setWorkflowId(workflowId);
    }

    @Override
    public void close() {
        DBOSContext context = DBOSContextHolder.get();
        context.setWorkflowId(previousWorkflowId);
    }
}

