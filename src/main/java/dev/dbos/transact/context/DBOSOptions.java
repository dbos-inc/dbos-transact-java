package dev.dbos.transact.context;

import dev.dbos.transact.queue.Queue;

public class DBOSOptions {

    private final boolean async;
    private final Queue queue;
    private final String workflowId;
    private final float timeoutSeconds ;

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

    public float getTimeout() {
        return timeoutSeconds;
    }

    public static class Builder {
        private boolean async = false;
        private Queue queue = null;
        private String workflowId = null;
        private float timeoutSeconds = 0f;

        public Builder async(boolean async) {
            this.async = async;
            return this;
        }

        public Builder queue(Queue queue) {
            this.queue = queue;
            return this;
        }

        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public Builder timeout(float timeout) {
            this.timeoutSeconds = timeout;
            return this;
        }

        public DBOSOptions build() {
            return new DBOSOptions(this);
        }
    }
}
