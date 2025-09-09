package dev.dbos.transact.context;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.queue.Queue;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSContext {

    private static final Logger logger = LoggerFactory.getLogger(DBOSContext.class);

    String assignedNextWorkflowId = null;
    String assignedQueueName = null;
    Duration assignedTimeout = null;

    // private String executorId = Constants.DEFAULT_EXECUTORID;

    // private final AtomicReference<DBOS> dbos;
    // private String parentWorkflowId;
    // private int parentFunctionId;
    // private volatile String workflowId;
    // private volatile int functionId;

    // private int currStepFunctionId;

    // private String authenticatedUser;
    // private String authenticateRole;
    // private String assumedRole;

    // private String appVersion;

    // // workflow timeouts
    // private Queue queue;
    // private long workflowTimeoutMs;
    // private long workflowDeadlineEpochMs;

    // // Queues
    // private String deduplicationId;
    // private int priority;

    // //
    // private volatile boolean inWorkflow = false;

    // private String stepId;

    // private boolean async;

    public DBOSContext() {
        // this.dbos = new AtomicReference<>();
    }

    public DBOSContext(String workflowId, int functionId) {
        throw new RuntimeException();
        // this.dbos = new AtomicReference<>();
        // this.workflowId = workflowId;
        // this.functionId = functionId;
        // this.inWorkflow = false;
    }

    public DBOSContext(WorkflowOptions options, int functionId) {
        throw new RuntimeException();
        // this.dbos = new AtomicReference<>();
        // this.workflowId = options.getWorkflowId();
        // this.functionId = functionId;
        // this.inWorkflow = false;
        // this.async = options.isAsync();
        // this.queue = options.getQueue();
        // this.workflowTimeoutMs = options.getTimeout() * 1000;
    }

    // private DBOSContext(DBOS dbos, String workflowId, int functionId, String
    // parentWorkflowId,
    // int parentFunctionId, boolean inWorkflow, boolean async, Queue q, long
    // timeout) {
    // this.dbos = new AtomicReference<>(dbos);
    // this.workflowId = workflowId;
    // this.functionId = functionId;
    // this.inWorkflow = inWorkflow;
    // this.parentWorkflowId = parentWorkflowId;
    // this.parentFunctionId = parentFunctionId;
    // this.async = async;
    // this.queue = q;
    // this.workflowTimeoutMs = timeout;
    // }

    // private DBOSContext(DBOS dbos, String childWorkflowId, String
    // parentWorkflowId, int parentFunctionId,
    // boolean async, Queue queue, long workflowTimeout) {
    // this.dbos = new AtomicReference<>(dbos);
    // this.workflowId = childWorkflowId;
    // this.parentWorkflowId = parentWorkflowId;
    // this.parentFunctionId = parentFunctionId;
    // this.inWorkflow = true;
    // this.async = async;
    // this.queue = queue;
    // this.workflowTimeoutMs = workflowTimeout;
    // }

    // private DBOSContext(DBOS dbos, WorkflowOptions options, String
    // parentWorkflowId, int parentFunctionId,
    // long parentTimeout) {
    // this.dbos = new AtomicReference<>(dbos);
    // this.workflowId = options.getWorkflowId();
    // this.parentWorkflowId = parentWorkflowId;
    // this.parentFunctionId = parentFunctionId;
    // this.inWorkflow = true;
    // this.async = options.isAsync();
    // this.queue = options.getQueue();
    // if (options.getTimeout() > 0) {
    // this.workflowTimeoutMs = options.getTimeout() * 1000;
    // } else {
    // this.workflowTimeoutMs = parentTimeout;
    // }
    // }

    public void setDbos(DBOS dbos) {
        throw new RuntimeException();

        // Objects.requireNonNull(dbos);
        // if (!this.dbos.compareAndSet(null, dbos)) {
        // if (this.dbos.get() != dbos) {
        // logger.error("setDbos collision {} {}",
        // System.identityHashCode(this.dbos.get()),
        // System.identityHashCode(dbos));
        // throw new IllegalStateException("DBOS instance already set and does not match
        // the provided instance.");
        // }
        // }
        // logger.debug("setDbos {}", System.identityHashCode(dbos));
    }

    public String getWorkflowId() {
        throw new RuntimeException();
        // return workflowId;
    }

    public String getParentWorkflowId() {
        throw new RuntimeException();
        // return parentWorkflowId;
    }

    // public void setWorkflowId(String workflowId) {
    // this.workflowId = workflowId;
    // }

    // public String getAuthenticatedUser() {
    // return authenticatedUser;
    // }

    // public void setAuthenticatedUser(String user) {
    // this.authenticatedUser = user;
    // }

    // public int getFunctionId() {
    // return functionId;
    // }

    public int getParentFunctionId() {
        throw new RuntimeException();
        // return parentFunctionId;
    }

    public int getAndIncrementFunctionId() {
        throw new RuntimeException();
        // return functionId++;
    }

    // public String getStepId() {
    // return stepId;
    // }

    // public void setStepId(String stepId) {
    // this.stepId = stepId;
    // }

    public DBOSContext copy() {
        throw new RuntimeException();
        // if (dbos.get() == null) {
        // throw new IllegalStateException("DBOS instance null in context to copy");
        // }
        // return new DBOSContext(dbos.get(), workflowId, functionId, parentWorkflowId,
        // parentFunctionId,
        // inWorkflow, async, queue, workflowTimeoutMs);
    }

    public DBOSContext createChild(String childWorkflowId) {
        throw new RuntimeException();
        // return new DBOSContext(dbos.get(), childWorkflowId, workflowId,
        // this.getAndIncrementFunctionId(),
        // this.async, this.getQueue(), this.workflowTimeoutMs);
    }

    public DBOSContext createChild(WorkflowOptions options) {
        throw new RuntimeException();
        // return new DBOSContext(dbos.get(), options, workflowId,
        // this.getAndIncrementFunctionId(),
        // this.workflowTimeoutMs);
    }

    public boolean hasParent() {
        throw new RuntimeException();
        // return this.parentWorkflowId != null;
    }

    public boolean isInWorkflow() {
        throw new RuntimeException();
        // return inWorkflow;
    }

    public void setInWorkflow(boolean in) {
        throw new RuntimeException();
        // this.inWorkflow = true;
    }

    public boolean isAsync() {
        throw new RuntimeException();
        // return async;
    }

    public Queue getQueue() {
        throw new RuntimeException();
        // return queue;
    }

    public long getWorkflowTimeoutMs() {
        throw new RuntimeException();
        // return workflowTimeoutMs;
    }

    public DBOSContext copyWithAsync() {
        throw new RuntimeException();
        // return new DBOSContext(dbos.get(), workflowId, functionId, parentWorkflowId,
        // parentFunctionId,
        // inWorkflow, true, queue, workflowTimeoutMs);
    }

    // public DBOSContext copyWithQueue(Queue q) {
    // return new DBOSContext(dbos.get(), workflowId, functionId, parentWorkflowId,
    // parentFunctionId,
    // inWorkflow, async, q, workflowTimeoutMs);
    // }

    public DBOSContext copyWithWorkflowId(String id) {
        throw new RuntimeException();
        // return new DBOSContext(dbos.get(), id, functionId, parentWorkflowId,
        // parentFunctionId, inWorkflow,
        // async, queue, workflowTimeoutMs);
    }

    public static Optional<String> workflowId() {
        return Optional.empty();
        // var holder = DBOSContextHolder.get();
        // return holder == null ? Optional.empty() :
        // Optional.ofNullable(holder.workflowId);
    }

    public static Optional<DBOS> dbosInstance() {
        return Optional.empty();
        // var holder = DBOSContextHolder.get();
        // return holder == null ? Optional.empty() :
        // Optional.ofNullable(holder.dbos.get());
    }

    public static boolean inWorkflow() {
        return false;
        // var holder = DBOSContextHolder.get();
        // return holder == null ? false : holder.inWorkflow;
    }
}
