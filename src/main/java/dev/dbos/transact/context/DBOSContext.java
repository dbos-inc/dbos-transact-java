package dev.dbos.transact.context;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.internal.DBOSContextHolder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSContext {

    private static final Logger logger = LoggerFactory.getLogger(DBOSContext.class);

    public record StepStatus(int functionId, OptionalInt currentAttempt, OptionalInt maxAttempts) {}
    public record WorkflowInfo(String workflowId, int functionId) {}

    DBOS dbos = null;

    String assignedNextWorkflowId = null;
    String authenticatedUser = null;
    List<String> authenticatedRoles = null;
    String assumedRole = null;
    Duration timeout = null;

    String workflowId = null;
    int functionId = 0;
    WorkflowInfo parent = null;
    StepStatus stepStatus = null;
    Instant deadline = null;

    public DBOSContext() { }

    public void setDbos(DBOS dbos) {
        Objects.requireNonNull(dbos);
        if (this.dbos != null) {
            if (this.dbos != dbos) {
                logger.error("setDbos collision {} {}", System.identityHashCode(this.dbos), System.identityHashCode(dbos));
                throw new IllegalStateException("DBOS instance already set and does not match the provided instance.");
            }
        } else {
            this.dbos = dbos;
        }
    }

    public boolean isInWorkflow() {
        return workflowId != null;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public int getAndIncrementFunctionId() {
        return functionId++;
    }

    public Duration getWorkflowTimeout() {
        return timeout;
    }

    public boolean hasParent() {
        return parent != null;
    }

    public String getParentWorkflowId() {
        return parent != null ? parent.workflowId() : null;
    }

    public int getParentFunctionId() {
        return parent != null ? parent.functionId() : 0;
    }

    public DBOSContext copy() {
        var ctx = new DBOSContext();

        ctx.dbos = dbos;

        ctx.assignedNextWorkflowId = assignedNextWorkflowId;
        ctx.authenticatedUser = authenticatedUser;
        ctx.authenticatedRoles = List.copyOf(authenticatedRoles);
        ctx.assumedRole = assumedRole;
        ctx.timeout = timeout;

        ctx.workflowId = workflowId;
        ctx.functionId = functionId;
        ctx.parent = parent;
        ctx.stepStatus = stepStatus;
        ctx.deadline = deadline;

        return ctx;
    }

    public DBOSContext createChild() {
        var ctx = new DBOSContext();
        ctx.dbos = dbos;
        ctx.assignedNextWorkflowId = assignedNextWorkflowId;
        assignedNextWorkflowId = null;
        ctx.authenticatedUser = authenticatedUser;
        ctx.authenticatedRoles = authenticatedRoles;
        ctx.assumedRole = assumedRole;
        ctx.parent = new WorkflowInfo(this.workflowId, this.functionId);
        return ctx;
    }


    public static Optional<String> workflowId() { 
        var ctx = DBOSContextHolder.get();
        return ctx == null ? Optional.empty() : Optional.ofNullable(ctx.workflowId);
    }

    public static Optional<DBOS> dbosInstance() {
        var ctx = DBOSContextHolder.get();
        return ctx == null ? Optional.empty() : Optional.ofNullable(ctx.dbos);
    }

    public static boolean inWorkflow() {
        var ctx = DBOSContextHolder.get();
        return ctx == null ? false : ctx.isInWorkflow();
    }
}
