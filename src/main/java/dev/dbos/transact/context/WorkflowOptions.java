package dev.dbos.transact.context;

import dev.dbos.transact.queue.Queue;

import java.time.Duration;
import java.util.Objects;

public record WorkflowOptions(
        String workflowId,
        String queueName,
        Duration timeout) {

    public static Builder builder() {
        return new Builder();
    }

    public Closer set() {
        var ctx = DBOSContextHolder.get();
        var closer = new Closer(ctx);
        ctx.assignedNextWorkflowId = Objects.requireNonNullElse(workflowId, ctx.assignedNextWorkflowId);
        ctx.assignedQueueName = Objects.requireNonNullElse(queueName, ctx.assignedQueueName);
        ctx.assignedTimeout = Objects.requireNonNullElse(timeout, ctx.assignedTimeout);
        return closer;
    }

    public static Closer setWorkflowId(String workflowId) {
        var options = new WorkflowOptions(Objects.requireNonNull(workflowId), null, null);
        return options.set();
    }

    public static class Closer implements AutoCloseable {

        private final DBOSContext ctx;
        private final String assignedNextWorkflowId;
        private final String assignedQueueName;
        private final Duration assignedTimeout;

        public Closer(DBOSContext ctx) {
            this.ctx = ctx;
            this.assignedNextWorkflowId = ctx.assignedNextWorkflowId;
            this.assignedQueueName = ctx.assignedQueueName;
            this.assignedTimeout = ctx.assignedTimeout;
        }

        @Override
        public void close() {
            ctx.assignedNextWorkflowId = assignedNextWorkflowId;
            ctx.assignedQueueName = assignedQueueName;
            ctx.assignedTimeout = assignedTimeout;
        }
    }

    public static class Builder {
        private String workflowId = null;
        private String queueName = null;
        private Duration timeout = null;

        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public Builder queue(Queue queue) {
            this.queueName = queue.getName();
            return this;
        }

        public Builder queue(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public WorkflowOptions build() {
            return new WorkflowOptions(workflowId, queueName, timeout);
        }
    }
}
