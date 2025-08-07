package dev.dbos.transact.execution;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.WorkflowInitResult;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.tempworkflows.InternalWorkflowsService;
import dev.dbos.transact.tempworkflows.InternalWorkflowsServiceImpl;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowHandleFuture;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSExecutor {

    private final DBOSConfig config;
    private SystemDatabase systemDatabase;
    private ExecutorService executorService;
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(2);
    private WorkflowRegistry workflowRegistry;
    private QueueService queueService;
    private InternalWorkflowsService internalWorkflowsService;
    Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

    public DBOSExecutor(DBOSConfig config, SystemDatabase sysdb) {
        this.config = config;
        this.systemDatabase = sysdb;
        this.executorService = Executors.newCachedThreadPool();
        this.workflowRegistry = new WorkflowRegistry();
    }

    public void setQueueService(QueueService queueService) {
        this.queueService = queueService;
    }

    public void shutdown() {
        workflowRegistry = null;
        executorService.shutdownNow();
        systemDatabase.destroy();
    }

    public void registerWorkflow(String workflowName, Object target, String targetClassName,
            Method method) {
        workflowRegistry.register(workflowName, target, targetClassName, method);
    }

    public WorkflowFunctionWrapper getWorkflow(String workflowName) {
        return workflowRegistry.get(workflowName);
    }

    public List<Queue> getAllQueuesSnapshot() {
        if (queueService == null) {
            throw new IllegalStateException("QueueService not set in DBOSExecutor");
        }
        return queueService.getAllQueuesSnapshot();
    }

    public WorkflowInitResult preInvokeWorkflow(String workflowName, String className,
            Object[] inputs, String workflowId, String queueName) {

        // TODO: queue deduplication and priority

        String inputString = JSONUtil.serializeArray(inputs);

        WorkflowState status = queueName == null ? WorkflowState.PENDING : WorkflowState.ENQUEUED;

        long workflowTimeoutMs = DBOSContextHolder.get().getWorkflowTimeoutMs();
        long workflowDeadlineEpoch = 0;
        if (workflowTimeoutMs > 0) {
            workflowDeadlineEpoch = System.currentTimeMillis() + workflowTimeoutMs;
        }

        WorkflowStatusInternal workflowStatusInternal = new WorkflowStatusInternal(workflowId,
                status, workflowName, className, null, null, null, null, null, null, null, null,
                queueName, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION, null, 0,
                workflowTimeoutMs, workflowDeadlineEpoch, null, 1, inputString);

        WorkflowInitResult initResult = null;
        try {
            initResult = systemDatabase.initWorkflowStatus(workflowStatusInternal, 3);
        } catch (Exception e) {
            logger.error("Error inserting into workflow_status", e);
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage(), e);
        }

        DBOSContext ctx = DBOSContextHolder.get();
        if (ctx.hasParent()) {
            systemDatabase.recordChildWorkflow(ctx.getParentWorkflowId(),
                    ctx.getWorkflowId(),
                    ctx.getParentFunctionId(),
                    workflowName);
        }

        return initResult;
    }

    public void postInvokeWorkflow(String workflowId, Object result) {

        String resultString = JSONUtil.serialize(result);
        systemDatabase.recordWorkflowOutput(workflowId, resultString);
    }

    public void postInvokeWorkflow(String workflowId, Throwable error) {

        SerializableException se = new SerializableException(error);
        String errorString = JSONUtil.serialize(se);

        systemDatabase.recordWorkflowError(workflowId, errorString);
    }

    public <T> T syncWorkflow(String workflowName, String targetClassName, Object target,
            Object[] args, WorkflowFunctionReflect function, String workflowId) throws Throwable {

        String wfid = workflowId;

        WorkflowInitResult initResult = null;

        DBOSContext ctx = DBOSContextHolder.get();
        if (ctx.hasParent()) {
            Optional<String> childId = systemDatabase.checkChildWorkflow(ctx.getParentWorkflowId(),
                    ctx.getParentFunctionId());
            if (childId.isPresent()) {
                return (T) systemDatabase.awaitWorkflowResult(childId.get());
            }
        }

        initResult = preInvokeWorkflow(workflowName, targetClassName, args, wfid, null);

        if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
            return (T) systemDatabase.getWorkflowResult(initResult.getWorkflowId()).get();
        } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
            logger.warn("Idempotency check not impl for error");
        } else if (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
            logger.warn("Idempotency check not impl for cancelled");
        }

        long allowedTime = initResult.getDeadlineEpochMS() - System.currentTimeMillis();
        if (initResult.getDeadlineEpochMS() > 0 && allowedTime < 0) {
            systemDatabase.cancelWorkflow(workflowId);
            return null;
        }

        if (allowedTime > 0) {
            ScheduledFuture<?> timeoutTask = timeoutScheduler.schedule(() -> {
                WorkflowStatus status = systemDatabase.getWorkflowStatus(wfid);
                if (status.getStatus() != WorkflowState.SUCCESS.name()
                        && status.getStatus() != WorkflowState.ERROR.name()) {
                    systemDatabase.cancelWorkflow(wfid);
                }
            }, allowedTime, TimeUnit.MILLISECONDS);
        }

        return runAndSaveResult(target, args, function, workflowId);
    }

    /**
     * Run and postInvoke reused separately preInvoke in reused separately
     *
     * @param target
     * @param args
     * @param function
     * @param workflowId
     * @return
     * @param <T>
     * @throws Throwable
     */
    <T> T runAndSaveResult(Object target, Object[] args, WorkflowFunctionReflect function,
            String workflowId) throws Throwable {

        try {

            @SuppressWarnings("unchecked")
            T result = (T) function.invoke(target, args);

            postInvokeWorkflow(workflowId, result);
            return result;
        } catch (Throwable e) {
            Throwable actual = (e instanceof InvocationTargetException)
                    ? ((InvocationTargetException) e).getTargetException()
                    : e;

            logger.error("Error in runWorkflow", actual);

            if (actual instanceof WorkflowCancelledException
                    || actual instanceof InterruptedException) {
                // don'nt mark the workflow status as error yet. this is cancel
                // if this is a parent cancel, the exception is thrown to caller
                // state is already c
                // if this is child cancel, its state is already Cancelled
                // in parent it will fall thru to PostInvoke call below to set state to
                // Error
                throw new AwaitedWorkflowCancelledException(workflowId);
            }

            postInvokeWorkflow(workflowId, actual);
            throw actual;
        }
    }

    public <T> WorkflowHandle<T> submitWorkflow(String workflowName, String targetClassName,
            Object target, Object[] args, WorkflowFunctionReflect function) throws Throwable {

        DBOSContext ctx = DBOSContextHolder.get();
        String workflowId = ctx.getWorkflowId();

        final String wfId = workflowId;

        if (ctx.hasParent()) {
            Optional<String> childId = systemDatabase.checkChildWorkflow(ctx.getParentWorkflowId(),
                    ctx.getParentFunctionId());
            if (childId.isPresent()) {
                logger.info("child Id is present " + childId);
                return new WorkflowHandleDBPoll<>(childId.get(), systemDatabase);
            }
        }

        WorkflowInitResult initResult = preInvokeWorkflow(workflowName,
                targetClassName,
                args,
                wfId,
                null);

        if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
            return new WorkflowHandleDBPoll<>(wfId, systemDatabase);
        } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
            logger.warn("Idempotency check not impl for error");
        } else if (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
            logger.warn("Idempotency check not impl for cancelled");
        }

        Callable<T> task = () -> {
            T result = null;

            // Doing this on purpose to ensure that we have the correct context
            String id = DBOSContextHolder.get().getWorkflowId();

            try {

                result = runAndSaveResult(target, args, function, id);

            } catch (Throwable e) {
                Throwable actual = (e instanceof InvocationTargetException)
                        ? ((InvocationTargetException) e).getTargetException()
                        : e;

                logger.error("Error executing workflow", actual);
            }

            return result;
        };

        long allowedTime = initResult.getDeadlineEpochMS() - System.currentTimeMillis();

        if (initResult.getDeadlineEpochMS() > 0 && allowedTime < 0) {
            logger.info("Timeout deadline exceeded. Cancelling workflow " + workflowId);
            systemDatabase.cancelWorkflow(workflowId);
            return new WorkflowHandleDBPoll<>(wfId, systemDatabase);
        }

        // Copy the context - dont just pass a reference - memory visibility
        ContextAwareCallable<T> contextAwareTask = new ContextAwareCallable<>(
                DBOSContextHolder.get().copy(), task);
        Future<T> future = executorService.submit(contextAwareTask);

        if (allowedTime > 0) {
            ScheduledFuture<?> timeoutTask = timeoutScheduler.schedule(() -> {
                if (!future.isDone()) {
                    logger.info(" Workflow timed out " + wfId);
                    future.cancel(false);
                    systemDatabase.cancelWorkflow(wfId);
                }
            }, allowedTime, TimeUnit.MILLISECONDS);
        }

        return new WorkflowHandleFuture<T>(workflowId, future, systemDatabase);
    }

    public void enqueueWorkflow(String workflowName, String targetClassName,
            WorkflowFunctionWrapper wrapper, Object[] args, Queue queue) throws Throwable {

        DBOSContext ctx = DBOSContextHolder.get();
        String wfid = ctx.getWorkflowId();

        if (wfid == null) {
            wfid = UUID.randomUUID().toString();
            ctx.setWorkflowId(wfid);
        }

        WorkflowInitResult initResult = null;
        try {
            initResult = preInvokeWorkflow(workflowName, targetClassName, args, wfid, queue.getName());

        } catch (Throwable e) {
            Throwable actual = (e instanceof InvocationTargetException)
                    ? ((InvocationTargetException) e).getTargetException()
                    : e;
            logger.error("Error enqueing workflow", actual);
            postInvokeWorkflow(initResult.getWorkflowId(), actual);
            throw actual;
        }
    }

    public <T> T runStep(String stepName, boolean retriedAllowed, int maxAttempts,
            float backOffRate, Object[] args, WorkflowFunction<T> function) throws Throwable {

        DBOSContext ctx = DBOSContextHolder.get();
        String workflowId = ctx.getWorkflowId();

        if (workflowId == null) {
            throw new DBOSException(UNEXPECTED.getCode(),
                    "No workflow id. Step must be called from workflow");
        }
        logger.info(String.format("Running step %s for workflow %s", stepName, workflowId));

        int stepFunctionId = ctx.getAndIncrementFunctionId();

        StepResult recordedResult = systemDatabase.checkStepExecutionTxn(workflowId,
                stepFunctionId,
                stepName);

        if (recordedResult != null) {

            String output = recordedResult.getOutput();
            if (output != null) {
                logger.info("Result has an output");
                Object[] stepO = JSONUtil.deserializeToArray(output);
                return stepO == null ? null : (T) stepO[0];
            }

            String error = recordedResult.getError();
            if (error != null) {
                // TODO: fix deserialization of errors
                throw new Exception(error);
            }
        }

        int currAttempts = 1;
        String serializedOutput = null;
        Throwable eThrown = null;
        T result = null;

        while (retriedAllowed && currAttempts <= maxAttempts) {

            try {
                result = function.execute();
                serializedOutput = JSONUtil.serialize(result);
                eThrown = null;
            } catch (Exception e) {
                // TODO: serialize
                Throwable actual = (e instanceof InvocationTargetException)
                        ? ((InvocationTargetException) e).getTargetException()
                        : e;
                logger.info("After: step threw exception " + actual.getMessage() + "-----"
                        + actual.toString());
                eThrown = actual;
            }

            ++currAttempts;
        }

        if (eThrown == null) {
            StepResult stepResult = new StepResult(workflowId, stepFunctionId, stepName,
                    serializedOutput, null);
            systemDatabase.recordStepResultTxn(stepResult);
            return result;
        } else {
            // TODO: serialize
            logger.info("After: step threw exception saving error " + eThrown.getMessage());
            StepResult stepResult = new StepResult(workflowId, stepFunctionId, stepName, null,
                    eThrown.getMessage());
            systemDatabase.recordStepResultTxn(stepResult);
            throw eThrown;
        }
    }

    /** Retrieve the workflowHandle for the workflowId */
    public <R> WorkflowHandle<R> retrieveWorkflow(String workflowId) {
        return new WorkflowHandleDBPoll(workflowId, systemDatabase);
    }

    public WorkflowHandle executeWorkflowById(String workflowId) {

        WorkflowStatus status = systemDatabase.getWorkflowStatus(workflowId);

        if (status == null) {
            logger.error("Workflow not found ", workflowId);
            throw new NonExistentWorkflowException(workflowId);
        }

        Object[] inputs = status.getInput();
        WorkflowFunctionWrapper functionWrapper = workflowRegistry.get(status.getName());

        if (functionWrapper == null) {
            throw new WorkflowFunctionNotFoundException(workflowId);
        }

        WorkflowHandle<?> handle = null;
        try (SetWorkflowID id = new SetWorkflowID(workflowId)) {
            DBOSContextHolder.get().setInWorkflow(true);
            try {
                handle = submitWorkflow(status.getName(),
                        functionWrapper.targetClassName,
                        functionWrapper.target,
                        inputs,
                        functionWrapper.function);
            } catch (Throwable t) {
                logger.error(String.format("Error executing workflow by id : %s", workflowId), t);
            }
        }

        return handle;
    }

    public void submit(Runnable task) {

        ContextAwareRunnable contextAwareTask = new ContextAwareRunnable(
                DBOSContextHolder.get().copy(), task);
        executorService.submit(contextAwareTask);
    }

    public void sleep(float seconds) {

        DBOSContext context = DBOSContextHolder.get();

        if (context.getWorkflowId() == null) {
            throw new DBOSException(ErrorCode.SLEEP_NOT_IN_WORKFLOW.getCode(),
                    "sleep() must be called from within a workflow");
        }

        systemDatabase.sleep(context.getWorkflowId(),
                context.getAndIncrementFunctionId(),
                seconds,
                false);
    }

    public <T> WorkflowHandle<T> resumeWorkflow(String workflowId) {

        Supplier<Void> resumeFunction = () -> {
            logger.info("Resuming workflow: ", workflowId);
            systemDatabase.resumeWorkflow(workflowId);
            return null; // void
        };
        // Execute the resume operation as a workflow step
        systemDatabase.callFunctionAsStep(resumeFunction, "DBOS.resumeWorkflow");
        return retrieveWorkflow(workflowId);
    }

    public void cancelWorkflow(String workflowId) {

        Supplier<Void> cancelFunction = () -> {
            logger.info("Cancelling workflow: ", workflowId);
            systemDatabase.cancelWorkflow(workflowId);
            return null; // void
        };
        // Execute the cancel operation as a workflow step
        systemDatabase.callFunctionAsStep(cancelFunction, "DBOS.resumeWorkflow");
    }

    public <T> WorkflowHandle<T> forkWorkflow(String workflowId, int startStep,
            ForkOptions options) {

        Supplier<String> forkFunction = () -> {
            logger.info(String.format("Forking workflow:%s from step:%d ", workflowId, startStep));

            return systemDatabase.forkWorkflow(workflowId, startStep, options);
        };

        String forkedId = systemDatabase.callFunctionAsStep(forkFunction, "DBOS.forkedWorkflow");
        return retrieveWorkflow(forkedId);
    }

    public <T> WorkflowHandle<T> startWorkflow(WorkflowFunction<T> func) {
        DBOSContext oldctx = DBOSContextHolder.get();
        DBOSContext newCtx = oldctx;

        if (newCtx.getWorkflowId() == null) {
            newCtx = newCtx.copyWithWorkflowId(UUID.randomUUID().toString());
        }

        if (newCtx.getQueue() == null) {
            newCtx = oldctx.copyWithAsync();
        }

        try {
            DBOSContextHolder.set(newCtx);
            func.execute();
            return retrieveWorkflow(newCtx.getWorkflowId());
        } catch (Throwable t) {
            throw new DBOSException(UNEXPECTED.getCode(), t.getMessage());
        } finally {
            DBOSContextHolder.set(oldctx);
        }
    }

    public InternalWorkflowsService getInternalWorkflowsService() {
        return internalWorkflowsService;
    }

    public InternalWorkflowsService createInternalWorkflowsService(DBOS dbos) {

        if (internalWorkflowsService != null) {
            logger.warn("InternalWorkflowsService already created.");
            return internalWorkflowsService;
        }

        internalWorkflowsService = dbos.<InternalWorkflowsService>Workflow()
                .interfaceClass(InternalWorkflowsService.class)
                .implementation(new InternalWorkflowsServiceImpl())
                .build();

        return internalWorkflowsService;

    }
}
