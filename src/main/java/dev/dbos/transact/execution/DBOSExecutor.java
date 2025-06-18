package dev.dbos.transact.execution;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.json.JSONUtil;
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

    public String preInvokeWorkflow(String workflowName,
                                  String interfaceName,
                                  String className,
                                  String methodName,
                                  Object[] inputs) {

        logger.info("In preInvokeWorkflow") ;

        String workflowId = UUID.randomUUID().toString();

        String inputString = JSONUtil.toJson(inputs);

        WorkflowStatusInternal workflowStatusInternal =
                new WorkflowStatusInternal(workflowId,
                        WorkflowStatus.PENDING,
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

        try {
            SystemDatabase.WorkflowInitResult initResult = systemDatabase.initWorkflowStatus(workflowStatusInternal, 3);
        } catch (SQLException e) {
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage(),e) ;
        }

        return workflowId ;
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
        String id = preInvokeWorkflow(workflowName, null, targetClassName, methodName, args);
        try {
            T result = function.execute();  // invoke the lambda
            logger.info("After: Workflow completed successfully");
            postInvokeWorkflow(id, result);
            return result;
        } catch (Throwable e) {
            Throwable actual = (e instanceof InvocationTargetException)
                    ? ((InvocationTargetException) e).getTargetException()
                    : e;
            postInvokeWorkflow(id, actual);
            throw actual;
        }
    }



}
