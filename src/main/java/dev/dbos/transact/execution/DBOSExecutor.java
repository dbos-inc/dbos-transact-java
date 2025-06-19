package dev.dbos.transact.execution;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.UUID;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

public class DBOSExecutor {

    private DBOSConfig config;
    private SystemDatabase systemDatabase;
    Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

    public DBOSExecutor(DBOSConfig config, SystemDatabase sysdb) {
        this.config = config;
        this.systemDatabase = sysdb ;

    }

    public void shutdown() {
        systemDatabase.destroy() ;
    }

    public SystemDatabase.WorkflowInitResult preInvokeWorkflow(String workflowName,
                                                               String interfaceName,
                                                               String className,
                                                               String methodName,
                                                               Object[] inputs) {

        logger.info("In preInvokeWorkflow") ;

        DBOSContext ctx = DBOSContextHolder.get();
        String workflowId = ctx.getWorkflowId() ;

        if (workflowId == null) {
            workflowId = UUID.randomUUID().toString();
        }

        String inputString = JSONUtil.toJson(inputs);

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
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage(),e) ;
        }

        return initResult;
    }

    public void postInvokeWorkflow(String workflowId, Object result) {

        String resultString = JSONUtil.toJson(result);


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
                             DBOSFunction<T> function) throws Throwable {


        SystemDatabase.WorkflowInitResult initResult = preInvokeWorkflow(workflowName, null, targetClassName, methodName, args);
        if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
            return (T) systemDatabase.getWorkflowResult(initResult.getWorkflowId()).get();
        } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
            logger.warn("Idempotency check not impl for error");
        } else if  (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
            logger.warn("Idempotency check not impl for cancelled");
        }


        try {
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



}
