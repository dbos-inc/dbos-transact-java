package dev.dbos.transact.context;

import dev.dbos.transact.internal.DBOSContextHolder;

import java.time.Duration;
import java.util.Objects;

public record WorkflowOptions(
        String workflowId,
        Duration timeout) {

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(String workflowId) {
        return builder().workflowId(workflowId);
    }

    public static WorkflowOptions fromWorkflowId(String workflowId) {
        return builder(workflowId).build();
    }

    public Closer set() {
        var ctx = DBOSContextHolder.get();
        var closer = new Closer(ctx);
        ctx.assignedNextWorkflowId = workflowId != null ? workflowId : ctx.assignedNextWorkflowId;
        ctx.timeout = timeout != null ? timeout : ctx.timeout;
        return closer;
    }

    public static Closer setWorkflowId(String workflowId) {
        var options = builder().workflowId(workflowId).build();
        return options.set();
    }

    public static class Closer implements AutoCloseable {

        private final DBOSContext ctx;
        private final String assignedNextWorkflowId;
        private final Duration timeout;

        public Closer(DBOSContext ctx) {
            this.ctx = ctx;
            this.assignedNextWorkflowId = ctx.assignedNextWorkflowId;
            this.timeout = ctx.timeout;
        }

        @Override
        public void close() {
            ctx.assignedNextWorkflowId = assignedNextWorkflowId;
            ctx.timeout = timeout;
        }
    }

    public static class Builder {
        private String workflowId = null;
        private Duration timeout = null;

        public Builder workflowId(String workflowId) {
            this.workflowId = Objects.requireNonNull(workflowId);
            return this;
        }

        public Builder timeout(Duration timeout) {
            if (Objects.requireNonNull(timeout).toNanos() <= 0) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            this.timeout = timeout;
            return this;
        }

        public WorkflowOptions build() {
            return new WorkflowOptions(workflowId, timeout);
        }
    }
}
