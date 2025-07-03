package dev.dbos.transact.context;

public class SetWorkflowID implements AutoCloseable {
    private final DBOSContext previousCtx ;

    public SetWorkflowID(String workflowId) {
        previousCtx = DBOSContextHolder.get();

        DBOSContext newCtx;

        if (previousCtx.getWorkflowId() != null ) {
            // we must be a child workflow
            newCtx = previousCtx.createChild(workflowId) ;
        } else {
            newCtx = new DBOSContext(workflowId, 0);
        }
        DBOSContextHolder.set(newCtx);
    }

    @Override
    public void close() {
        DBOSContextHolder.set(previousCtx) ;
        //TODO : for child workflows we need like a SetInheritedContext that keeps function id from parent
    }
}

