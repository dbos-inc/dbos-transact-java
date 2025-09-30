package dev.dbos.transact.database;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.InsertWorkflowResult;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.*;
import java.util.*;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemDatabase implements AutoCloseable {

  private static Logger logger = LoggerFactory.getLogger(SystemDatabase.class);
  private final HikariDataSource dataSource;

  private final WorkflowDAO workflowDAO;
  private final StepsDAO stepsDAO;
  private final QueuesDAO queuesDAO;
  private final NotificationsDAO notificationsDAO;
  private final NotificationService notificationService;

  public SystemDatabase(DBOSConfig config) {
    this(SystemDatabase.createDataSource(config, null));
  }

  public SystemDatabase(HikariDataSource dataSource) {
    this.dataSource = dataSource;
    stepsDAO = new StepsDAO(dataSource);
    workflowDAO = new WorkflowDAO(dataSource);
    queuesDAO = new QueuesDAO(dataSource);
    notificationService = new NotificationService(dataSource);
    notificationsDAO = new NotificationsDAO(dataSource, notificationService);
  }

  @Override
  public void close() throws Exception {
    dataSource.close();
  }

  public void start() {
    notificationService.start();
  }

  public void stop() {
    notificationService.stop();
  }

  /**
   * Get workflow result by workflow ID
   *
   * @param workflowId The workflow UUID
   * @return Optional containing the raw output string if workflow completed successfully, empty
   *     otherwise
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
      WorkflowStatusInternal initStatus, Integer maxRetries) throws SQLException {

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
      Connection connection, WorkflowStatusInternal status) throws SQLException {
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

  public Optional<WorkflowStatus> getWorkflowStatus(String workflowId) {

    return workflowDAO.getWorkflowStatus(workflowId);
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) throws SQLException {

    return workflowDAO.listWorkflows(input);
  }

  public List<GetPendingWorkflowsOutput> getPendingWorkflows(String executorId, String appVersion)
      throws SQLException {
    return workflowDAO.getPendingWorkflows(executorId, appVersion);
  }

  public boolean clearQueueAssignment(String workflowId) throws SQLException {
    return queuesDAO.clearQueueAssignment(workflowId);
  }

  public StepResult checkStepExecutionTxn(String workflowId, int functionId, String functionName)
      throws IllegalStateException, WorkflowCancelledException, UnexpectedStepException {

    try {
      try (Connection connection = dataSource.getConnection()) {
        return StepsDAO.checkStepExecutionTxn(workflowId, functionId, functionName, connection);
      }
    } catch (SQLException sq) {
      logger.error("Unexpected SQL exception", sq);
      throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
    }
  }

  public void recordStepResultTxn(StepResult result) {

    try {
      StepsDAO.recordStepResultTxn(dataSource, result);
    } catch (SQLException sq) {
      logger.error("Unexpected SQL exception", sq);
      throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
    }
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) throws SQLException {

    return stepsDAO.listWorkflowSteps(workflowId);
  }

  public Object awaitWorkflowResult(String workflowId) throws Exception {
    return workflowDAO.awaitWorkflowResult(workflowId);
  }

  public List<String> getAndStartQueuedWorkflows(Queue queue, String executorId, String appVersion)
      throws SQLException {
    return queuesDAO.getAndStartQueuedWorkflows(queue, executorId, appVersion);
  }

  public List<WorkflowStatus> listQueuedWorkflows(ListQueuedWorkflowsInput input, boolean loadInput)
      throws SQLException {

    return queuesDAO.getQueuedWorkflows(input, loadInput);
  }

  public void recordChildWorkflow(
      String parentId,
      String childId, // workflowId of the
      // child
      int functionId, // func id in the parent
      String functionName) {
    workflowDAO.recordChildWorkflow(parentId, childId, functionId, functionName);
  }

  public Optional<String> checkChildWorkflow(String workflowUuid, int functionId) {

    try {
      return workflowDAO.checkChildWorkflow(workflowUuid, functionId);
    } catch (SQLException sq) {
      throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
    }
  }

  public void send(
      String workflowId, int functionId, String destinationId, Object message, String topic) {

    try {
      notificationsDAO.send(workflowId, functionId, destinationId, message, topic);
    } catch (SQLException sq) {
      logger.error("Sql Exception", sq);
      throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
    }
  }

  public Object recv(
      String workflowId,
      int functionId,
      int timeoutFunctionId,
      String topic,
      double timeoutSeconds) {

    try {
      return notificationsDAO.recv(
          workflowId, functionId, timeoutFunctionId, topic, timeoutSeconds);
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

  public Object getEvent(
      String targetId, String key, double timeoutSeconds, GetWorkflowEventContext callerCtx) {

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
      throw new DBOSException(ErrorCode.RESUME_WORKFLOW_ERROR.getCode(), s.getMessage());
    }
  }

  public String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options) {

    try {
      return workflowDAO.forkWorkflow(originalWorkflowId, startStep, options);
    } catch (SQLException sq) {
      throw new DBOSException(ErrorCode.RESUME_WORKFLOW_ERROR.getCode(), sq.getMessage());
    }
  }

  public void garbageCollect(Long cutoffEpochTimestampMs, Long rowsThreshold) {
    try {
      workflowDAO.garbageCollect(cutoffEpochTimestampMs, rowsThreshold);
    } catch (SQLException sq) {
      logger.error("Unexpected SQL exception", sq);
      throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
    }
  }

  public static HikariDataSource createDataSource(
      String url, String user, String password, int poolSize, int timeout) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(url);
    hikariConfig.setUsername(user);
    hikariConfig.setPassword(password);
    hikariConfig.setMaximumPoolSize(poolSize > 0 ? poolSize : 2);
    if (timeout > 0) {
      hikariConfig.setConnectionTimeout(timeout);
    }

    return new HikariDataSource(hikariConfig);
  }

  public static HikariDataSource createDataSource(DBOSConfig config, String dbName) {

    if (dbName == null) {
      if (config.sysDbName() != null) {
        dbName = config.sysDbName();
      } else {
        dbName = config.name() + Constants.SYS_DB_SUFFIX;
      }
    }

    String dburl = config.url();

    if (dburl == null) {
      dburl = String.format("jdbc:postgresql://%s:%d/%s", config.dbHost(), config.dbPort(), dbName);
    }

    String dbUser = config.dbUser();
    String dbPassword = config.dbPassword();
    int maximumPoolSize = config.maximumPoolSize();
    int connectionTimeout = config.connectionTimeout();

    return createDataSource(dburl, dbUser, dbPassword, maximumPoolSize, connectionTimeout);
  }

  public static HikariDataSource createPostgresDataSource(DBOSConfig config) {
    HikariConfig hikariConfig = new HikariConfig();

    String dburl = config.url();

    if (dburl != null) {
      dburl = createPostgresConnectionUrl(dburl);
    } else {

      dburl =
          String.format(
              "jdbc:postgresql://%s:%d/%s",
              config.dbHost(), config.dbPort(), Constants.POSTGRES_DEFAULT_DB);
    }

    String dbUser = config.dbUser();
    String dbPassword = config.dbPassword();
    hikariConfig.setJdbcUrl(dburl);
    hikariConfig.setUsername(dbUser);
    hikariConfig.setPassword(dbPassword);

    hikariConfig.setMaximumPoolSize(2);
    return new HikariDataSource(hikariConfig);
  }

  public static DataSource createDataSource(DBOSConfig config) {
    return createDataSource(config, null);
  }

  Connection getSysDBConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public static String createPostgresConnectionUrl(String originalUrl) {
    if (originalUrl == null || !originalUrl.startsWith("jdbc:postgresql://")) {
      throw new IllegalArgumentException("Invalid PostgreSQL JDBC URL: " + originalUrl);
    }

    String urlWithoutPrefix = originalUrl.substring("jdbc:postgresql://".length());

    // Find the database name part (after the last '/' and before '?' if it exists)
    int slashIndex = urlWithoutPrefix.lastIndexOf('/');
    if (slashIndex == -1) {
      throw new IllegalArgumentException(
          "Invalid JDBC URL format - missing database name: " + originalUrl);
    }

    String hostAndPort = urlWithoutPrefix.substring(0, slashIndex);
    String databaseAndParams = urlWithoutPrefix.substring(slashIndex + 1);

    // Split database name from query parameters
    String queryParams = "";
    int questionMarkIndex = databaseAndParams.indexOf('?');
    if (questionMarkIndex != -1) {
      queryParams = databaseAndParams.substring(questionMarkIndex);
    }

    // Build new URL with 'postgres' database
    return "jdbc:postgresql://" + hostAndPort + "/postgres" + queryParams;
  }
}
