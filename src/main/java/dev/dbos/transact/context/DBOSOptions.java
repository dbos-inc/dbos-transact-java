package dev.dbos.transact.context;

import dev.dbos.transact.queue.Queue;

import java.util.UUID;

public class DBOSOptions {

    private final boolean async;
    private final Queue queue;
    private final String workflowId;
    private final long timeoutSeconds ;

    private DBOSOptions(Builder builder) {
        this.async = builder.async;
        this.queue = builder.queue;
        this.workflowId = builder.workflowId;
        this.timeoutSeconds = builder.timeoutSeconds;
    }

    public boolean isAsync() {
        return async;
    }

    public Queue getQueue() {
        return queue;
    }


    public String getWorkflowId() {
        return workflowId;
    }


    public long getTimeout() {
        return timeoutSeconds;
    }

    public static class Builder {
        private boolean async = false;
        private Queue queue = null;
        private String workflowId = null;
        private long timeoutSeconds = 0;

        public Builder(String workflowId) {
            this.workflowId = workflowId;
        }

        public Builder async() {
            this.async = true;
            return this;
        }

        public Builder queue(Queue queue) {
            this.queue = queue;
            return this;
        }


        public Builder timeout(long timeout) {
            this.timeoutSeconds = timeout;
            return this;
        }

        public DBOSOptions build() {
            return new DBOSOptions(this);
        }
    }
}
