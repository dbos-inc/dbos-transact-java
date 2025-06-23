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
            System.out.println(e.getMessage()) ;
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage(),e) ;
        }

        logger.info("leaving preinvoke") ;
        return initResult;
    }

    public void postInvokeWorkflow(String workflowId, Object result) {


        String resultString = JSONUtil.serialize(result);



        systemDatabase.recordWorkflowOutput(workflowId, resultString);

        logger.info("In post Invoke workflow with result") ;
    }

    public void postInvokeWorkflow(String workflowId, Throwable error) {

        String errorString = error.toString() ;

        systemDatabase.recordWorkflowError(workflowId, errorString);

        logger.info("In post Invoke workflow with error") ;
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
                wfid = UUID.randomUUID().toString();
            }
        }


        SystemDatabase.WorkflowInitResult initResult = preInvokeWorkflow(workflowName, null,
                                                            targetClassName, methodName, args, wfid);
        logger.info("returned from preInvoke") ;
        if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
            return (T) systemDatabase.getWorkflowResult(initResult.getWorkflowId()).get();
        } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
            logger.warn("Idempotency check not impl for error");
        } else if  (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
            logger.warn("Idempotency check not impl for cancelled");
        }


        try {
            logger.info("Before executing workflow") ;
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
        }

        final String wfId = workflowId ;

        Callable<T> task = () -> {
            T result = null ;
            logger.info("Callable executing the workflow.. " + wfId);
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

        Future<T> future = executorService.submit(task);

        return new WorkflowHandleFuture<T>(workflowId, future, systemDatabase);

    }

}
