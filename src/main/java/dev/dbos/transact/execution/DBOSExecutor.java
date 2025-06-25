package dev.dbos.transact.execution;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.json.JSONUtil;
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
import java.sql.SQLException;
import java.util.Optional;
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
    Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

    public DBOSExecutor(DBOSConfig config, SystemDatabase sysdb) {
        this.config = config;
        this.systemDatabase = sysdb ;
        this.executorService = Executors.newCachedThreadPool();

    }

    public void shutdown() {
        systemDatabase.destroy() ;
    }

    public SystemDatabase.WorkflowInitResult preInvokeWorkflow(String workflowName,
                                                               String interfaceName,
                                                               String className,
                                                               String methodName,
                                                               Object[] inputs,
                                                               String workflowId) {

        logger.info("In preInvokeWorkflow") ;

        String inputString = JSONUtil.serialize(inputs) ;

        WorkflowStatusInternal workflowStatusInternal =
                new WorkflowStatusInternal(workflowId,
                        WorkflowState.PENDING,
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
                        null,
                        null,
                        null,
                        null,
                        0,
                        300000,
                        System.currentTimeMillis() + 2400000,
                        null,
                        1,
                        inputString) ;

        SystemDatabase.WorkflowInitResult initResult = null;
        try {
             initResult = systemDatabase.initWorkflowStatus(workflowStatusInternal, 3);
        } catch (SQLException e) {
            logger.error("Error inserting into workflow_status", e);
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage(),e) ;
        }

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
                             String methodName,
                             Object[] args,
                             DBOSFunction<T> function,
                             String workflowId) throws Throwable {

        String wfid = workflowId ;

        if (wfid == null) {
            DBOSContext ctx = DBOSContextHolder.get();
            wfid = ctx.getWorkflowId() ;

            if (wfid == null) {
                logger.info("mjjjj wfid is nulllllll") ;
                wfid = UUID.randomUUID().toString();
                ctx.setWorkflowId(wfid);
            } else {
                logger.info("workflowId from context ", wfid);
            }
        }

        SystemDatabase.WorkflowInitResult initResult = null;
        try {

            initResult = preInvokeWorkflow(workflowName, null,
                                                            targetClassName, methodName, args, wfid);

            if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
                return (T) systemDatabase.getWorkflowResult(initResult.getWorkflowId()).get();
            } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
                logger.warn("Idempotency check not impl for error");
            } else if  (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
                logger.warn("Idempotency check not impl for cancelled");
            }

            logger.info("Before executing workflow " + DBOSContextHolder.get().getWorkflowId()) ;
            T result = function.execute();  // invoke the lambda
            logger.info("After: Workflow completed successfully");
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
                                                String methodName,
                                                Object[] args,
                                                DBOSFunction<T> function) throws Throwable {

        DBOSContext ctx = DBOSContextHolder.get();
        String workflowId = ctx.getWorkflowId() ;

        if (workflowId == null) {
            workflowId = UUID.randomUUID().toString();
            ctx.setWorkflowId(workflowId);
        }

        final String wfId = workflowId ;

        Callable<T> task = () -> {
            T result = null ;
            System.out.println("Thread ID in task.call(): " + Thread.currentThread().getId());
            System.out.println("workflowId just before log = " + DBOSContextHolder.get().getWorkflowId());
            logger.info("Callable executing the workflow.. " + wfId);
            logger.info("From the contextCallable executing the workflow.. " + DBOSContextHolder.get().getWorkflowId());

            try {

                result = runWorkflow(workflowName,
                        targetClassName,
                        methodName,
                        args,
                        function,
                        wfId);


            } catch (Throwable e) {
                Throwable actual = (e instanceof InvocationTargetException)
                        ? ((InvocationTargetException) e).getTargetException()
                        : e;

                logger.error("Error executing workflow", actual);

            }

            return result ;
        };

        ContextAwareCallable<T> contextAwareTask = new ContextAwareCallable<>(task);

        System.out.println("mjjjj in main thread: " + DBOSContextHolder.get());
        System.out.println("mjjjj in main thread wfId: " + DBOSContextHolder.get().getWorkflowId());

        /*ContextAwareCallable<T> contextAwareTask = new ContextAwareCallable<>(() -> {

            System.out.println("Thread ID in task.call(): " + Thread.currentThread().getId());
            System.out.println("DBOSContextHolder.get(): " + DBOSContextHolder.get());
            System.out.println("workflowId: " + DBOSContextHolder.get().getWorkflowId());
            System.out.println("Context object hash: " + System.identityHashCode(DBOSContextHolder.get()));


            System.out.println("mjjjjj WFID in task: " + DBOSContextHolder.get().getWorkflowId());
            T result = null ;
            System.out.println("Thread ID in task.call(): " + Thread.currentThread().getId());
            System.out.println("workflowId just before log = " + DBOSContextHolder.get().getWorkflowId());
            logger.info("Callable executing the workflow.. " + wfId);
            logger.info("From the contextCallable executing the workflow.. " + DBOSContextHolder.get().getWorkflowId());

            try {

                result = runWorkflow(workflowName,
                        targetClassName,
                        methodName,
                        args,
                        function,
                        wfId);


            } catch (Throwable e) {
                Throwable actual = (e instanceof InvocationTargetException)
                        ? ((InvocationTargetException) e).getTargetException()
                        : e;

                logger.error("Error executing workflow", actual);

            }

            return result ;
        }); */


        // contextAwareTask.setDBOSContext(DBOSContextHolder.get());
        contextAwareTask.setWorkflowId(DBOSContextHolder.get().getWorkflowId());
        Future<T> future = executorService.submit(contextAwareTask);

        return new WorkflowHandleFuture<T>(workflowId, future, systemDatabase);

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

}
