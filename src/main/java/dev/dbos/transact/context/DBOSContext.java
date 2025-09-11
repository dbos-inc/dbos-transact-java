package dev.dbos.transact.context;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.internal.DBOSContextHolder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSContext {

    private static final Logger logger = LoggerFactory.getLogger(DBOSContext.class);

    public record StepStatus(int functionId, OptionalInt currentAttempt, OptionalInt maxAttempts) {
    }

    private final DBOS dbos;

    String assignedNextWorkflowId = null;
    Duration timeout = null;

    private final String workflowId;
    private int functionId = -1;

    private String parentWorkflowId = null;
    private int parentFunctionId = -1;

    StepStatus stepStatus = null;
    Instant deadline = null;

    public DBOSContext() {
        dbos = null;
        workflowId = null;
    }

    public DBOSContext(DBOS dbos, String workflowId) {
        this.dbos = Objects.requireNonNull(dbos);
        this.workflowId = workflowId != null ? workflowId : UUID.randomUUID().toString();
    }

    // public void setDbos(DBOS dbos) {
    //     Objects.requireNonNull(dbos);
    //     if (this.dbos != null) {
    //         if (this.dbos != dbos) {
    //             logger.error("setDbos collision {} {}",
    //                     System.identityHashCode(this.dbos),
    //                     System.identityHashCode(dbos));
    //             throw new IllegalStateException("DBOS instance already set and does not match the provided instance.");
    //         }
    //     } else {
    //         this.dbos = dbos;
    //     }
    // }

    public Optional<String> getNextWorkflowId() { 
        return getNextWorkflowId(Optional.empty());
    }

    public Optional<String> getNextWorkflowId(Optional<String> assignedId) {
        if (assignedId.isPresent()) { return assignedId; }

        if (assignedNextWorkflowId != null) {
            var workflowId = Optional.of(assignedNextWorkflowId);
            assignedNextWorkflowId = null;
            return workflowId;
        }

        return Optional.empty();
    }

    public boolean isInWorkflow() {
        return workflowId != null;
    }

    public boolean isInStep() {
        return stepStatus != null;
    }

    public String getWorkflowId() {
        // return workflowId;
        throw new RuntimeException();
    }

    // public int getNextFunctionId() {
    //     return functionId++;
    // }

    public Duration getTimeout() {
        return timeout;
    }

    public Instant getDeadline() {
        return deadline;
    }

    public boolean hasParent() {
        return parentWorkflowId != null;
    }

    public String getParentWorkflowId() {
        return parentWorkflowId;
    }

    public int getParentFunctionId() {
        return parentFunctionId;
    }

    public int getAndIncrementFunctionId() {
        throw new RuntimeException();
    }

    // public DBOSContext copy() {
    //     var ctx = new DBOSContext();

    //     ctx.dbos = dbos;

    //     ctx.assignedNextWorkflowId = assignedNextWorkflowId;
    //     ctx.authenticatedUser = authenticatedUser;
    //     ctx.authenticatedRoles = List.copyOf(authenticatedRoles);
    //     ctx.assumedRole = assumedRole;
    //     ctx.timeout = timeout;

    //     ctx.workflowId = workflowId;
    //     ctx.functionId = functionId;
    //     ctx.parent = parent;
    //     ctx.stepStatus = stepStatus;
    //     ctx.deadline = deadline;

    //     return ctx;
    // }

    // public DBOSContext createChild() {
    //     var ctx = new DBOSContext();
    //     ctx.dbos = dbos;
    //     ctx.assignedNextWorkflowId = assignedNextWorkflowId;
    //     assignedNextWorkflowId = null;
    //     ctx.authenticatedUser = authenticatedUser;
    //     ctx.authenticatedRoles = authenticatedRoles;
    //     ctx.assumedRole = assumedRole;
    //     ctx.parent = new WorkflowInfo(this.workflowId, this.functionId);
    //     return ctx;
    // }

    public static Optional<String> workflowId() {
        var ctx = DBOSContextHolder.get();
        return Optional.ofNullable(ctx.workflowId);
    }

    public static Optional<DBOS> dbosInstance() {
        var ctx = DBOSContextHolder.get();
        return Optional.ofNullable(ctx.dbos);
    }

    public static boolean inWorkflow() {
        return DBOSContextHolder.get().isInWorkflow();
    }
}
