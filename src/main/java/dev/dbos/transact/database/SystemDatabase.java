package dev.dbos.transact.database;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

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
    this(SystemDatabase.createDataSource(config));
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
  public void close() {
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
   */
  public Optional<String> getWorkflowResult(String workflowId) {
    return DbRetry.call(
        () -> {
          return workflowDAO.getWorkflowResult(workflowId);
        });
  }

  /**
   * Initializes the status of a workflow.
   *
   * @param initStatus The initial workflow status details.
   * @param maxRetries Optional maximum number of retries.
   * @return An object containing the current status and optionally the deadline epoch milliseconds.
   * @throws DBOSWorkflowConflictException If a conflicting workflow already exists.
   * @throws DBOSDeadLetterQueueException If the workflow exceeds max retries.
   */
  public WorkflowInitResult initWorkflowStatus(
      WorkflowStatusInternal initStatus, Integer maxRetries) {
    return DbRetry.call(
        () -> {
          return workflowDAO.initWorkflowStatus(initStatus, maxRetries);
        });
  }

  /**
   * Insert into the workflow_status table
   *
   * @param status @WorkflowStatusInternal holds the data for a workflow_status row
   * @return @InsertWorkflowResult some of the column inserted
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

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return DbRetry.call(
        () -> {
          return workflowDAO.listWorkflows(input);
        });
  }

  public List<GetPendingWorkflowsOutput> getPendingWorkflows(String executorId, String appVersion) {
    return DbRetry.call(
        () -> {
          return workflowDAO.getPendingWorkflows(executorId, appVersion);
        });
  }

  public boolean clearQueueAssignment(String workflowId) {
    return DbRetry.call(
        () -> {
          return queuesDAO.clearQueueAssignment(workflowId);
        });
  }

  public StepResult checkStepExecutionTxn(String workflowId, int functionId, String functionName)
      throws IllegalStateException, DBOSWorkflowCancelledException, DBOSUnexpectedStepException {

    return DbRetry.call(
        () -> {
          try (Connection connection = dataSource.getConnection()) {
            return StepsDAO.checkStepExecutionTxn(workflowId, functionId, functionName, connection);
          }
        });
  }

  public void recordStepResultTxn(StepResult result) {
    DbRetry.run(
        () -> {
          StepsDAO.recordStepResultTxn(dataSource, result);
        });
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) {
    return DbRetry.call(
        () -> {
          return stepsDAO.listWorkflowSteps(workflowId);
        });
  }

  public <T, E extends Exception> T awaitWorkflowResult(String workflowId) throws E {
    return workflowDAO.<T, E>awaitWorkflowResult(workflowId);
  }

  public List<String> getAndStartQueuedWorkflows(
      Queue queue, String executorId, String appVersion) {
    return DbRetry.call(
        () -> {
          return queuesDAO.getAndStartQueuedWorkflows(queue, executorId, appVersion);
        });
  }

  public List<WorkflowStatus> listQueuedWorkflows(
      ListQueuedWorkflowsInput input, boolean loadInput) {
    return DbRetry.call(
        () -> {
          return queuesDAO.getQueuedWorkflows(input, loadInput);
        });
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
    return DbRetry.call(
        () -> {
          return workflowDAO.checkChildWorkflow(workflowUuid, functionId);
        });
  }

  public void send(
      String workflowId, int functionId, String destinationId, Object message, String topic) {

    DbRetry.run(
        () -> {
          notificationsDAO.send(workflowId, functionId, destinationId, message, topic);
        });
  }

  public Object recv(
      String workflowId,
      int functionId,
      int timeoutFunctionId,
      String topic,
      double timeoutSeconds) {

    return DbRetry.call(
        () -> {
          try {
            return notificationsDAO.recv(
                workflowId, functionId, timeoutFunctionId, topic, timeoutSeconds);
          } catch (InterruptedException ie) {
            logger.error("recv() was interrupted", ie);
            throw new DBOSException(UNEXPECTED.getCode(), ie.getMessage());
          }
        });
  }

  public void setEvent(String workflowId, int functionId, String key, Object message) {

    DbRetry.run(
        () -> {
          notificationsDAO.setEvent(workflowId, functionId, key, message);
        });
  }

  public Object getEvent(
      String targetId, String key, double timeoutSeconds, GetWorkflowEventContext callerCtx) {

    return DbRetry.call(
        () -> {
          return notificationsDAO.getEvent(targetId, key, timeoutSeconds, callerCtx);
        });
  }

  public double sleep(String workflowId, int functionId, double seconds, boolean skipSleep) {
    return DbRetry.call(
        () -> {
          return stepsDAO.sleep(workflowId, functionId, seconds, skipSleep);
        });
  }

  public void cancelWorkflow(String workflowId) {
    DbRetry.run(
        () -> {
          workflowDAO.cancelWorkflow(workflowId);
        });
  }

  public void resumeWorkflow(String workflowId) {
    DbRetry.run(
        () -> {
          workflowDAO.resumeWorkflow(workflowId);
        });
  }

  public String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options) {
    return DbRetry.call(
        () -> {
          return workflowDAO.forkWorkflow(originalWorkflowId, startStep, options);
        });
  }

  public void garbageCollect(Long cutoffEpochTimestampMs, Long rowsThreshold) {
    DbRetry.run(
        () -> {
          workflowDAO.garbageCollect(cutoffEpochTimestampMs, rowsThreshold);
        });
  }

  Connection getSysDBConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public static HikariDataSource createDataSource(String url, String user, String password) {
    return createDataSource(url, user, password, 0, 0);
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

  public static HikariDataSource createDataSource(DBOSConfig config) {
    var dburl = config.databaseUrl();
    var dbUser = config.dbUser();
    var dbPassword = config.dbPassword();
    var maximumPoolSize = config.maximumPoolSize();
    var connectionTimeout = config.connectionTimeout();

    return createDataSource(dburl, dbUser, dbPassword, maximumPoolSize, connectionTimeout);
  }
}
