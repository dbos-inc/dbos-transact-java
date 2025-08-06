package dev.dbos.transact.context;

import java.io.Closeable;

public class SetDBOSOptions implements Closeable {

    private final DBOSContext previousCtx;

    public SetDBOSOptions(DBOSOptions options) {

        if (options.getWorkflowId() == null) {
            throw new IllegalArgumentException(
                    "Workflow Id cannot be null with SetDBOSOptions. ");
        }

        previousCtx = DBOSContextHolder.get();

        DBOSContext newCtx;

        if (previousCtx.getWorkflowId() != null) {
            // we must be a child workflow
            newCtx = previousCtx.createChild(options);
        }
        else {
            newCtx = new DBOSContext(options, 0);
        }
        DBOSContextHolder.set(newCtx);
    }

    @Override
    public void close() {
        DBOSContextHolder.set(previousCtx);
    }
}
