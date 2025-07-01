package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.WorkflowInitResult;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.exceptions.WorkflowFunctionNotFoundException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.QueueRegistry;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowHandleFuture;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

public class DBOSExecutor {

    private DBOSConfig config;
    private SystemDatabase systemDatabase;
    private ExecutorService executorService ;
    private WorkflowRegistry workflowRegistry ;
    // private QueueRegistry queueRegistry ;
    private QueueService queueService;
    Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

    public DBOSExecutor(DBOSConfig config, SystemDatabase sysdb) {
        this.config = config;
        this.systemDatabase = sysdb ;
        this.executorService = Executors.newCachedThreadPool();
        // this.executorService = Executors.newFixedThreadPool(20);
        System.out.println("Creating new registry");
        this.workflowRegistry = new WorkflowRegistry() ;
        // this.queueRegistry = new QueueRegistry();
    }

    public void setQueueService(QueueService queueService) {
        this.queueService = queueService;
    }

    public void shutdown() {
        workflowRegistry = null ;
        systemDatabase.destroy() ;
    }

    public void registerWorkflow(String workflowName, Object target, String targetClassName, Method method) {
        workflowRegistry.register(workflowName, target, targetClassName, method);
    }

    public WorkflowFunctionWrapper getWorkflow(String workflowName) {
        return workflowRegistry.get(workflowName);
    }


    public WorkflowInitResult preInvokeWorkflow(String workflowName,
                                                String className,
                                                Object[] inputs,
                                                String workflowId,
                                                String queueName) {

        // logger.info("In preInvokeWorkflow with " + workflowId) ;

        // TODO: queue deduplication and priority

        String inputString = JSONUtil.serialize(inputs) ;

        WorkflowState status = queueName == null ? WorkflowState.PENDING : WorkflowState.ENQUEUED;

        WorkflowStatusInternal workflowStatusInternal =
                new WorkflowStatusInternal(workflowId,
                        status,
                        workflowName,
                        className,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        queueName,
                        Constants.DEFAULT_EXECUTORID,
                        Constants.DEFAULT_APP_VERSION,
                        null,
                        0,
                        300000,
                        System.currentTimeMillis() + 2400000,
                        null,
                        1,
                        inputString) ;

        WorkflowInitResult initResult = null;
        try {
             initResult = systemDatabase.initWorkflowStatus(workflowStatusInternal, 3);
        } catch (Exception e) {
            logger.error("Error inserting into workflow_status", e);
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage(),e) ;
        }

        // logger.info("Successfully completed preInvokeWorkflow") ;
        return initResult;
    }

    public void postInvokeWorkflow(String workflowId, Object result) {

        String resultString = JSONUtil.serialize(result);
        systemDatabase.recordWorkflowOutput(workflowId, resultString);

    }

    public void postInvokeWorkflow(String workflowId, Throwable error) {

        String errorString = error.toString() ;

        systemDatabase.recordWorkflowError(workflowId, errorString);

    }

    public <T> T runWorkflow(String workflowName,
                             String targetClassName,
                             Object target,
                             Object[] args,
                             WorkflowFunction function,
                             String workflowId) throws Throwable {

        String wfid = workflowId ;

        if (wfid == null) {
            DBOSContext ctx = DBOSContextHolder.get();
            wfid = ctx.getWorkflowId() ;

            if (wfid == null) {
                wfid = UUID.randomUUID().toString();
                ctx.setWorkflowId(wfid);
            }
        }

        WorkflowInitResult initResult = null;
        try {

            initResult = preInvokeWorkflow(workflowName, targetClassName,  args, wfid, null);

            if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
                return (T) systemDatabase.getWorkflowResult(initResult.getWorkflowId()).get();
            } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
                logger.warn("Idempotency check not impl for error");
            } else if  (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
                logger.warn("Idempotency check not impl for cancelled");
            }

            // logger.info("Before executing workflow " + DBOSContextHolder.get().getWorkflowId()) ;
            //T result = function.execute();  // invoke the lambda
            @SuppressWarnings("unchecked")
            T result = (T) function.invoke(target, args);
            // logger.info("After: Workflow completed successfully");
            postInvokeWorkflow(initResult.getWorkflowId(), result);
            return result;
        } catch (Throwable e) {
            Throwable actual = (e instanceof InvocationTargetException)
                    ? ((InvocationTargetException) e).getTargetException()
                    : e;
            postInvokeWorkflow(initResult.getWorkflowId(), actual);
            throw actual;
        }
    }

    public <T> WorkflowHandle<T> submitWorkflow(String workflowName,
                                                String targetClassName,
                                                Object target,
                                                Object[] args,
                                                WorkflowFunction function) throws Throwable {

        DBOSContext ctx = DBOSContextHolder.get();
        String workflowId = ctx.getWorkflowId() ;

        if (workflowId == null) {
            workflowId = UUID.randomUUID().toString();
            ctx.setWorkflowId(workflowId);
        }

        final String wfId = workflowId ;

        Callable<T> task = () -> {
            T result = null ;

            // Doing this on purpose to ensure that we have the correct context
            String id = DBOSContextHolder.get().getWorkflowId();

            // logger.info("Callable executing the workflow.. " + id);

            try {

                result = runWorkflow(workflowName,
                        targetClassName,
                        target,
                        args,
                        function,
                        // wfId); doing it the hard way
                        id);


            } catch (Throwable e) {
                Throwable actual = (e instanceof InvocationTargetException)
                        ? ((InvocationTargetException) e).getTargetException()
                        : e;

                logger.error("Error executing workflow", actual);

            }

            return result ;
        };

        // Copy the context - dont just pass a reference - memory visibility
        ContextAwareCallable<T> contextAwareTask = new ContextAwareCallable<>(DBOSContextHolder.get().copy(),task);
        Future<T> future = executorService.submit(contextAwareTask);

        return new WorkflowHandleFuture<T>(workflowId, future, systemDatabase);

    }

    public void enqueueWorkflow(String workflowName,
                                                String targetClassName,
                                                WorkflowFunctionWrapper wrapper,
                                                Object[] args,
                                                 Queue queue
                                                ) throws Throwable {



        DBOSContext ctx = DBOSContextHolder.get();
        String wfid = ctx.getWorkflowId() ;

        if (wfid == null) {
            wfid = UUID.randomUUID().toString();
            ctx.setWorkflowId(wfid);
        }
        WorkflowInitResult initResult = null;
        try {
            initResult = preInvokeWorkflow(workflowName, targetClassName,  args, wfid, queue.getName());

        } catch (Throwable e) {
            Throwable actual = (e instanceof InvocationTargetException)
                    ? ((InvocationTargetException) e).getTargetException()
                    : e;
            postInvokeWorkflow(initResult.getWorkflowId(), actual);
            throw actual;
        }

    }


    public <T> T runStep(String stepName,
                             boolean retriedAllowed,
                             int maxAttempts,
                             float backOffRate,
                             Object[] args,
                             DBOSFunction<T> function
                         ) throws Throwable {


        DBOSContext ctx = DBOSContextHolder.get();
        String workflowId = ctx.getWorkflowId();

        if (workflowId == null) {
            throw new DBOSException(UNEXPECTED.getCode(), "No workflow id. Step must be called from workflow");
        }
        logger.info(String.format("Running step %s for workflow %s", stepName, workflowId)) ;

        int stepFunctionId = ctx.getAndIncrementFunctionId() ;

        StepResult recordedResult = systemDatabase.checkStepExecutionTxn(workflowId, stepFunctionId, stepName) ;

        if (recordedResult != null) {

            String output = recordedResult.getOutput() ;
            if (output != null) {
                return (T) JSONUtil.deserialize(output) ;
            }

            String error = recordedResult.getError();
            if (error != null) {
                // TODO: fix deserialization of errors
                throw new Exception(error);
            }
        }

        int currAttempts = 1 ;
        String serializedOutput = null ;
        Throwable eThrown  = null ;
        T result = null ;

        while (retriedAllowed && currAttempts <= maxAttempts) {

            try {
                logger.info("Before executing step");
                result = function.execute();
                logger.info("After: step completed successfully " + result);// invoke the lambda
                serializedOutput = JSONUtil.serialize(result);
                logger.info("Json serialized output is " + serializedOutput);
                eThrown = null ;
            } catch(Exception e) {
                // TODO: serialize
                Throwable actual = (e instanceof InvocationTargetException)
                        ? ((InvocationTargetException) e).getTargetException()
                        : e;
                logger.info("After: step threw exception " + actual.getMessage() + "-----" + actual.toString()) ;
                eThrown = actual;
            }

            ++currAttempts;
        }

        if (eThrown == null) {
            StepResult stepResult = new StepResult(workflowId, stepFunctionId, stepName, serializedOutput, null);
            systemDatabase.recordStepResultTxn(stepResult);
            return result;
        } else {
            // TODO: serialize
            logger.info("After: step threw exception saving error " + eThrown.getMessage()) ;
            StepResult stepResult = new StepResult(workflowId, stepFunctionId, stepName, null, eThrown.getMessage());
            systemDatabase.recordStepResultTxn(stepResult);
            throw eThrown;
        }
    }


    /**
     * Retrieve the workflowHandle for the workflowId
     *
     */
    public WorkflowHandle retrieveWorkflow(String workflowId) {
        return new WorkflowHandleDBPoll(workflowId, systemDatabase) ;
    }

    public WorkflowHandle executeWorkflowById(String workflowId) {

        WorkflowStatus status = systemDatabase.getWorkflowStatus(workflowId) ;

        if (status == null) {
            logger.error("Workflow not found ", workflowId);
            throw new NonExistentWorkflowException(workflowId) ;
        }

        Object[] inputs = status.getInput() ;
        WorkflowFunctionWrapper functionWrapper = workflowRegistry.get(status.getName()) ;

        if (functionWrapper == null) {
            throw new WorkflowFunctionNotFoundException(workflowId) ;
        }

        WorkflowHandle handle = null ;
        try (SetWorkflowID id = new SetWorkflowID(workflowId)) {
            try {
                handle = submitWorkflow(status.getName(), functionWrapper.targetClassName, functionWrapper.target, inputs, functionWrapper.function);
            } catch (Throwable t) {
                logger.error(String.format("Error executing workflow by id : %s", workflowId) , t);
            }
        }

        return handle ;

    }

    public void submit(Runnable task) {

        ContextAwareRunnable contextAwareTask = new ContextAwareRunnable(DBOSContextHolder.get().copy(),task);
        executorService.submit(contextAwareTask);

    }

}
