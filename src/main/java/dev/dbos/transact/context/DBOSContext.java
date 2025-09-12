package dev.dbos.transact.context;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.internal.DBOSContextHolder;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

public class DBOSContext {

    // private static final Logger logger = LoggerFactory.getLogger(DBOSContext.class);

    public record WorkflowInfo(String workflowId, int functionId) {
    }

    public record StepStatus(int functionId, OptionalInt currentAttempt, OptionalInt maxAttempts) {
    }

    private final DBOS dbos;

    String assignedNextWorkflowId = null;
    Duration timeout = null;

    private final String workflowId;
    private int functionId = -1;

    private final WorkflowInfo parent;
    private StepStatus stepStatus = null;
    private Instant deadline = null;

    public DBOSContext() {
        dbos = null;
        workflowId = null;
        parent = null;
    }

    public DBOSContext(DBOS dbos, String workflowId) {
        this.dbos = Objects.requireNonNull(dbos);
        this.workflowId = workflowId != null ? workflowId : UUID.randomUUID().toString();
        functionId = 0;
        parent = null;
    }

    public DBOSContext(DBOS dbos, String workflowId, String parentWorkflowId, int parentFunctionId) {
        this.dbos = Objects.requireNonNull(dbos);
        this.workflowId = workflowId != null ? workflowId : UUID.randomUUID().toString();
        functionId = 0;
        this.parent = new WorkflowInfo(Objects.requireNonNull(parentWorkflowId), parentFunctionId);
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

    public String getNextWorkflowId() { 
        return getNextWorkflowId(null);
    }

    public String getNextWorkflowId(String assignedId) {
        if (assignedId != null) { return assignedId; }

        if (assignedNextWorkflowId != null) {
            var workflowId = assignedNextWorkflowId;
            assignedNextWorkflowId = null;
            return workflowId;
        }

        return null;
    }

    
    public boolean isInWorkflow() {
        return workflowId != null;
    }

    public String getWorkflowId() {
        if (workflowId == null) { 
            throw new IllegalAccessError("accessing workflow ID outside of a workflow");
        }
        return workflowId;
    }

    public int getNextFunctionId() {
        if (workflowId == null) { 
            throw new IllegalAccessError("accessing function ID outside of a workflow");
        }
        return functionId++;
    }



    public boolean isInStep() {
        return stepStatus != null;
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

    public WorkflowInfo getParent() {
        return parent;
    }

    public DBOSContext makeTop(DBOS dbos, String workflowId) {
        assert this.dbos == null;
        assert this.workflowId == null;

        var ctx = new DBOSContext(dbos, workflowId);

        ctx.assignedNextWorkflowId = assignedNextWorkflowId;
        ctx.timeout = timeout;
        ctx.deadline = deadline;

        return ctx;
    }

    public DBOSContext makeChild(DBOS dbos, String workflowId, String parentWorkflowId, int parentFunctionId) {
        assert this.dbos == dbos;
        assert this.workflowId != null;

        var ctx = new DBOSContext(dbos, workflowId, parentWorkflowId, parentFunctionId);
        ctx.assignedNextWorkflowId = assignedNextWorkflowId;
        ctx.timeout = timeout;
        ctx.deadline = deadline;

        return ctx;
    }


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
