package dev.dbos.transact.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.notifications.GetWorkflowEventContext;
import dev.dbos.transact.notifications.NotificationService;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.InsertWorkflowResult;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

public class SystemDatabase {

    private Logger logger = LoggerFactory.getLogger(SystemDatabase.class) ;
    private DBOSConfig config ;
    private static SystemDatabase instance ;
    private DataSource dataSource ;
    private WorkflowDAO workflowDAO;
    private StepsDAO stepsDAO ;
    private QueuesDAO queuesDAO;
    private NotificationService notificationService;
    private NotificationsDAO notificationsDAO ;

    private SystemDatabase(DBOSConfig cfg) {
        config = cfg ;

        /* String dbName;
        if (config.getSysDbName() != null) {
            dbName = config.getSysDbName();
        } else {
            dbName = config.getName() + Constants.SYS_DB_SUFFIX;
        } */

        dataSource= SystemDatabase.createDataSource(config, null);
        stepsDAO = new StepsDAO(dataSource) ;
        workflowDAO = new WorkflowDAO(dataSource) ;
        queuesDAO = new QueuesDAO(dataSource) ;
        notificationService = new NotificationService(dataSource, this) ;
        notificationsDAO = new NotificationsDAO(dataSource, stepsDAO, notificationService) ;
    }

    private SystemDatabase(DataSource ds) {

        this.dataSource = ds ;
        workflowDAO = new WorkflowDAO(dataSource) ;
        stepsDAO = new StepsDAO(dataSource) ;
        queuesDAO = new QueuesDAO(dataSource) ;
        notificationService = new NotificationService(dataSource, this) ;
        notificationsDAO = new NotificationsDAO(dataSource, stepsDAO, notificationService) ;
    }

    public static synchronized void initialize(DBOSConfig cfg) {
        if (instance != null) {
            throw new IllegalStateException("SystemDatabase has already been initialized.");
        }
        instance = new SystemDatabase(cfg);
    }

    public static synchronized void initialize(DataSource ds) {
        if (instance != null) {
            throw new IllegalStateException("SystemDatabase has already been initialized.");
        }
        instance = new SystemDatabase(ds);
    }

    public static SystemDatabase getInstance() {
        if (instance == null) {
            throw new RuntimeException("SystemDatabase should be initalized first") ;
        }
        return instance ;
    }

    public synchronized static void destroy() {
        if (instance.dataSource != null) {
            ((HikariDataSource)instance.dataSource).close();
        }
        instance = null ;
    }

    public void setNotificationService(NotificationService service) {
        notificationService = service;
        notificationsDAO = new NotificationsDAO(dataSource, stepsDAO, service) ;
    }

    public NotificationService getNotificationService() {
        return notificationService ;
    }


    /**
     * Get workflow result by workflow ID
     * @param workflowId The workflow UUID
     * @return Optional containing the raw output string if workflow completed successfully, empty otherwise
     * @throws SQLException if database operation fails
     */
    public Optional<String> getWorkflowResult(String workflowId) throws SQLException {
        return workflowDAO.getWorkflowResult(workflowId);
    }

    /**
     * Initializes the status of a workflow.
     *
     * @param initStatus The initial workflow status details.
     * @param maxRetries Optional maximum number of retries.
     * @return An object containing the current status and optionally the deadline epoch milliseconds.
     * @throws SQLException If a database error occurs.
     * @throws DBOSWorkflowConflictException If a conflicting workflow already exists.
     * @throws DBOSDeadLetterQueueException If the workflow exceeds max retries.
     */
    public WorkflowInitResult initWorkflowStatus(
            WorkflowStatusInternal initStatus,
            Integer maxRetries
    ) throws SQLException {

             return workflowDAO.initWorkflowStatus(initStatus, maxRetries);
    }

    /**
     * Insert into the workflow_status table
     *
     * @param status @WorkflowStatusInternal holds the data for a workflow_status row
     * @return @InsertWorkflowResult some of the column inserted
     * @throws SQLException
     */

    public InsertWorkflowResult insertWorkflowStatus(
            Connection connection,
            WorkflowStatusInternal status
    ) throws SQLException {
        return workflowDAO.insertWorkflowStatus(connection, status);

    }


    /**
     * Store the result to workflow_status
     *
     * @param workflowId id of the workflow
     * @param result output serialized as json
     */
    public void recordWorkflowOutput(String workflowId, String result) {
        workflowDAO.recordWorkflowOutput(workflowId, result);
    }

    /**
     * Store the error to workflow_status
     *
     * @param workflowId id of the workflow
     * @param error output serialized as json
     */
    public void recordWorkflowError(String workflowId, String error) {
        workflowDAO.recordWorkflowError(workflowId, error);
    }

    public WorkflowStatus getWorkflowStatus(String workflowId) {

        return workflowDAO.getWorkflowStatus(workflowId);
    }

    public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) throws SQLException {
        return workflowDAO.listWorkflows(input);
    }

    public List<GetPendingWorkflowsOutput> getPendingWorkflows(String executorId, String appVersion)
            throws SQLException {
        return workflowDAO.getPendingWorkflows(executorId, appVersion) ;
    }

    public StepResult checkStepExecutionTxn(
            String workflowId,
            int functionId,
            String functionName
    ) throws IllegalStateException, WorkflowCancelledException, UnExpectedStepException {

        try {
            try (Connection connection = dataSource.getConnection()) {
                return stepsDAO.checkStepExecutionTxn(workflowId, functionId, functionName, connection);
            }
        } catch(SQLException sq) {
            logger.error("Unexpected SQL exception", sq) ;
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage()) ;
        }
    }

    public void recordStepResultTxn(StepResult result)  {

        try {
            stepsDAO.recordStepResultTxn(result);
        } catch(SQLException sq) {
            logger.error("Unexpected SQL exception", sq) ;
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage()) ;
        }

    }

    public List<StepInfo> listWorkflowSteps(String workflowId) {
        try {
            return stepsDAO.listWorkflowSteps(workflowId);
        } catch(SQLException sq) {
            logger.error("Unexpected SQL exception", sq) ;
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage()) ;
        }

    }

    public Object awaitWorkflowResult(String workflowId) {

        return workflowDAO.awaitWorkflowResult(workflowId) ;

    }

    public List<String> getAndStartQueuedWorkflows(Queue queue, String executorId, String appVersion) throws SQLException {
        return queuesDAO.getAndStartQueuedWorkflows(queue, executorId, appVersion);
    }

    public void recordChildWorkflow(String parentId,
                                    String childId, // workflowId of the child
                                    int functionId, // func id in the parent
                                    String functionName) {
        workflowDAO.recordChildWorkflow(parentId, childId, functionId, functionName);
    }

    public Optional<String> checkChildWorkflow(String workflowUuid, int functionId) {

        try {
            return workflowDAO.checkChildWorkflow(workflowUuid, functionId) ;
        } catch (SQLException sq) {
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
        }

    }

    public void send(String workflowId, int functionId, String destinationId,
                     Object message, String topic)  {

        try {
            notificationsDAO.send(workflowId, functionId, destinationId, message, topic);
        } catch(SQLException sq) {
            logger.error("Sql Exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
        }
    }

    public Object recv(String workflowId, int functionId, int timeoutFunctionId,
                       String topic, double timeoutSeconds)  {

        try {
            return notificationsDAO.recv(workflowId, functionId, timeoutFunctionId, topic, timeoutSeconds) ;
        } catch (SQLException sq) {
            logger.error("Sql Exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
        } catch (InterruptedException ie) {
            logger.error("recv() was interrupted", ie);
            throw new DBOSException(UNEXPECTED.getCode(), ie.getMessage());
        }

    }

    public void setEvent(String workflowId, int functionId, String key, Object message) {

        try {
            notificationsDAO.setEvent(workflowId, functionId, key, message);
        } catch (SQLException sq) {
            logger.error("Sql Exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
        }
    }

    public Object getEvent(String targetId, String key, double timeoutSeconds, GetWorkflowEventContext callerCtx) {

        try {
            return notificationsDAO.getEvent(targetId, key, timeoutSeconds, callerCtx);
        } catch (SQLException sq) {
            logger.error("Sql Exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
        }
    }

    public double sleep(String workflowId, int functionId, double seconds, boolean skipSleep) {

        try {
            return stepsDAO.sleep(workflowId, functionId, seconds, skipSleep);
        } catch (SQLException sq) {
            logger.error("Sql Exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
        }

    }

    public void cancelWorkflow(String workflowId) {
        try {
            workflowDAO.cancelWorkflow(workflowId);
        } catch (SQLException sq) {
            logger.error("Sql Exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
        }

    }

    public void resumeWorkflow(String workflowId) {
        try {
            workflowDAO.resumeWorkflow(workflowId);
        } catch (SQLException s) {
            throw new DBOSException(ErrorCode.RESUME_WORKFLOW_ERROR.getCode(), s.getMessage()) ;
        }

    }

    public String forkWorkflow(String originalWorkflowId,
                               int startStep,
                               ForkOptions options) {

        try {
            return workflowDAO.forkWorkflow(originalWorkflowId, startStep, options) ;
        } catch (SQLException sq) {
            throw new DBOSException(ErrorCode.RESUME_WORKFLOW_ERROR.getCode(), sq.getMessage()) ;
        }
    }


    public <T> T callFunctionAsStep(Supplier<T> fn, String functionName)  {
        DBOSContext ctx = DBOSContextHolder.get();

        int nextFuncId = 0 ;

        if (ctx != null && ctx.isInWorkflow()) {
            nextFuncId = ctx.getAndIncrementFunctionId() ;

            StepResult result = null ;

            try (Connection connection = dataSource.getConnection()) {
                result = stepsDAO.checkStepExecutionTxn(
                        ctx.getWorkflowId(), nextFuncId, functionName, connection
                );
            } catch(SQLException e) {
                throw new DBOSException(UNEXPECTED.getCode(), "Function execution failed: " + functionName, e);
            }

            if (result != null) {
                return handleExistingResult(result, functionName);
            }
        }

        T functionResult;
        try {

            try {
                functionResult = fn.get();
            } catch (Exception e) {
                if (ctx != null && ctx.isInWorkflow()) {
                    String jsonError = JSONUtil.serializeError(e);
                    StepResult r = new StepResult(ctx.getWorkflowId(), nextFuncId, functionName, null, jsonError);
                    stepsDAO.recordStepResultTxn(r);
                }

                if ( e instanceof NonExistentWorkflowException) {
                    throw e;
                } else {
                    throw new DBOSException(UNEXPECTED.getCode(), "Function execution failed: " + functionName, e);
                }
            }

            // If we're in a workflow, record the successful result
            if (ctx != null && ctx.isInWorkflow()) {
                String jsonOutput = JSONUtil.serialize(functionResult);
                StepResult o = new StepResult(ctx.getWorkflowId(), nextFuncId, functionName, jsonOutput, null);
                stepsDAO.recordStepResultTxn(o);
            }
        } catch(SQLException sq) {
            throw new DBOSException(UNEXPECTED.getCode(), "Function execution failed: " + functionName, sq);
        }

        return functionResult;
    }

    @SuppressWarnings("unchecked")
    private <T> T handleExistingResult(StepResult result, String functionName) {
        if (result.getOutput() != null) {
            Object[] resArray = JSONUtil.deserializeToArray(result.getOutput());
            return resArray == null ? null : (T) resArray[0];
        } else if (result.getError() != null) {
            Object[] eArray = JSONUtil.deserializeToArray(result.getError());
            SerializableException se = (SerializableException) eArray[0];
            throw new DBOSAppException(String.format("Exception of type %s", se.className), se) ;
        } else {
            throw new IllegalStateException(
                    String.format("Recorded output and error are both null for %s", functionName)
            );
        }
    }

    public static DataSource createDataSource(DBOSConfig config, String dbName) {
        HikariConfig hikariConfig = new HikariConfig();

        if (dbName == null) {
            if (config.getSysDbName() != null) {
                dbName = config.getSysDbName();
            } else {
                dbName = config.getName() + Constants.SYS_DB_SUFFIX;
            }
        }

        String dburl = System.getenv(Constants.JDBC_URL_ENV_VAR) ;

        if (config.getUrl() != null) {
            dburl = config.getUrl();
        }

        if (dburl == null) {
            dburl = String.format("jdbc:postgresql://%s:%d/%s", config.getDbHost(), config.getDbPort(), dbName);
        }

        String dbUser = System.getenv(Constants.POSTGRES_USER_ENV_VAR) ;
        if (config.getDbUser() != null) {
            dbUser = config.getDbUser() ;
        }

        String dbPassword = System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR);
        if (config.getDbPassword() != null) {
            dbPassword = config.getDbPassword();
        }

        hikariConfig.setJdbcUrl(dburl);
        hikariConfig.setUsername(dbUser);
        hikariConfig.setPassword(dbPassword);

        int maximumPoolSize = config.getMaximumPoolSize();
        if (maximumPoolSize > 0) {
            hikariConfig.setMaximumPoolSize(maximumPoolSize);
        } else {
            hikariConfig.setMaximumPoolSize(2);
        }

        int connectionTimeout = config.getConnectionTimeout();
        if (connectionTimeout > 0) {
            hikariConfig.setConnectionTimeout(connectionTimeout);
        }

        return new HikariDataSource(hikariConfig);
    }




    Connection getSysDBConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
