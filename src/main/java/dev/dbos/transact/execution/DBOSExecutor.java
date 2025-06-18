package dev.dbos.transact.execution;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void preInvokeWorkflow(String workflowName,
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

    }

    public void postInvokeWorkflow() {

        logger.info("In post Invoke workflow") ;
    }


}
