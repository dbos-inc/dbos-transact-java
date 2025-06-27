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
        DBOSContextHolder.clear();
        //TODO : needs some work restore the previous context
        // Need a SetContext
        // save the old context
        // set a new context with new values
        // on close restore the old context
    }
}

