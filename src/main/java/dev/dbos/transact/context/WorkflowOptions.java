package dev.dbos.transact.context;

import dev.dbos.transact.internal.DBOSContextHolder;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public record WorkflowOptions(
        String workflowId,
        Duration timeout,
        String authenticatedUser,
        List<String> authenticatedRoles,
        String assumedRole
    ) {

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
        ctx.assignedNextWorkflowId = Objects.requireNonNullElse(workflowId, ctx.assignedNextWorkflowId);
        ctx.timeout = Objects.requireNonNullElse(timeout, ctx.timeout);
        ctx.authenticatedUser = Objects.requireNonNullElse(authenticatedUser, ctx.authenticatedUser);
        ctx.authenticatedRoles = Objects.requireNonNullElse(List.copyOf(authenticatedRoles), ctx.authenticatedRoles);
        ctx.assumedRole = Objects.requireNonNullElse(assumedRole, ctx.assumedRole);
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
        private final String authenticatedUser;
        private final List<String> authenticatedRoles;
        private final String assumedRole;

        public Closer(DBOSContext ctx) {
            this.ctx = ctx;
            this.assignedNextWorkflowId = ctx.assignedNextWorkflowId;
            this.timeout = ctx.timeout;
            this.authenticatedUser = ctx.authenticatedUser;
            this.authenticatedRoles = List.copyOf(ctx.authenticatedRoles);
            this.assumedRole = ctx.assumedRole;
        }

        @Override
        public void close() {
            ctx.assignedNextWorkflowId = assignedNextWorkflowId;
            ctx.timeout = timeout;
            ctx.authenticatedUser = authenticatedUser;
            ctx.authenticatedRoles = authenticatedRoles;
            ctx.assumedRole = assumedRole;
        }
    }

    public static class Builder {
        private String workflowId = null;
        private Duration timeout = null;
        private String authenticatedUser = null;
        private List<String> authenticatedRoles = null;
        private String assumedRole = null;

        public Builder workflowId(String workflowId) {
            this.workflowId = Objects.requireNonNull(workflowId);
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout);
            return this;
        }

        public Builder authenticatedUser(String authenticatedUser) {
            this.authenticatedUser = Objects.requireNonNull(authenticatedUser);
            return this;
        }

        public Builder authenticatedRoles(List<String> authenticatedRoles) {
            this.authenticatedRoles = List.copyOf(Objects.requireNonNull(authenticatedRoles));
            return this;
        }

        public Builder assumedRole(String assumedRole) {
            this.assumedRole = Objects.requireNonNull(assumedRole);
            return this;
        }

        public WorkflowOptions build() {
            return new WorkflowOptions(workflowId, timeout, authenticatedUser, authenticatedRoles, assumedRole);
        }
    }
}
