package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemDatabase implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SystemDatabase.class);

  public static String sanitizeSchema(String schema) {
    return Objects.requireNonNullElse(schema, Constants.DB_SCHEMA).replace("\0", "");
  }

  private final DataSource dataSource;
  private final String schema;
  private final boolean created;
  private final DBOSSerializer serializer;

  private final WorkflowDAO workflowDAO;
  private final StepsDAO stepsDAO;
  private final QueuesDAO queuesDAO;
  private final NotificationsDAO notificationsDAO;
  private final NotificationService notificationService;
  private final SchedulesDAO schedulesDAO;
  private final ApplicationVersionDAO applicationVersionDAO;
  private final ExternalStateDAO externalStateDAO;

  private SystemDatabase(
      DataSource dataSource, String schema, boolean created, DBOSSerializer serializer) {
    schema = sanitizeSchema(schema);
    if (schema.contains("\"")) {
      throw new IllegalArgumentException("Schema name must not contain double quotes");
    }

    this.schema = schema;
    this.dataSource = dataSource;
    this.created = created;
    this.serializer = serializer;

    stepsDAO = new StepsDAO(dataSource, this.schema, serializer);
    workflowDAO = new WorkflowDAO(dataSource, this.schema, serializer, stepsDAO);
    queuesDAO = new QueuesDAO(dataSource, this.schema);
    schedulesDAO = new SchedulesDAO(dataSource, this.schema, serializer);
    notificationService = new NotificationService(dataSource);
    notificationsDAO =
        new NotificationsDAO(dataSource, notificationService, this.schema, serializer);
    applicationVersionDAO = new ApplicationVersionDAO(dataSource, this.schema);
    externalStateDAO = new ExternalStateDAO(dataSource, this.schema);
  }

  public SystemDatabase(String url, String user, String password, String schema) {
    this(createDataSource(url, user, password), schema, true, null);
  }

  public SystemDatabase(
      String url, String user, String password, String schema, DBOSSerializer serializer) {
    this(createDataSource(url, user, password), schema, true, serializer);
  }

  public SystemDatabase(DataSource dataSource, String schema) {
    this(dataSource, schema, false, null);
  }

  public SystemDatabase(DataSource dataSource, String schema, DBOSSerializer serializer) {
    this(dataSource, schema, false, serializer);
  }

  public static SystemDatabase create(DBOSConfig config) {
    if (config.dataSource() == null) {
      return new SystemDatabase(
          config.databaseUrl(),
          config.dbUser(),
          config.dbPassword(),
          config.databaseSchema(),
          config.serializer());
    } else {
      return new SystemDatabase(config.dataSource(), config.databaseSchema(), config.serializer());
    }
  }

  Optional<HikariConfig> getConfig() {
    if (dataSource instanceof HikariDataSource hds) {
      return Optional.of(hds);
    }
    return Optional.empty();
  }

  public static HikariDataSource createDataSource(DBOSConfig config) {
    return createDataSource(config.databaseUrl(), config.dbUser(), config.dbPassword());
  }

  public static HikariDataSource createDataSource(String url, String user, String password) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(user);
    config.setPassword(password);

    config.setMaxLifetime(60_000);
    config.setKeepaliveTime(30000);
    config.setConnectionTimeout(10000);
    config.setValidationTimeout(2000);
    config.setInitializationFailTimeout(-1);
    config.setMaximumPoolSize(10);
    config.setMinimumIdle(10);

    config.addDataSourceProperty("tcpKeepAlive", "true");
    config.addDataSourceProperty("connectTimeout", "10");
    config.addDataSourceProperty("socketTimeout", "60");
    config.addDataSourceProperty("reWriteBatchedInserts", "true");

    return new HikariDataSource(config);
  }

  @Override
  public void close() {
    notificationService.stop();
    if (created && dataSource instanceof HikariDataSource hikariDataSource) {
      hikariDataSource.close();
    }
  }

  public void start() {
    notificationService.start();
  }

  void speedUpPollingForTest() {
    workflowDAO.speedUpPollingForTest();
    notificationsDAO.speedUpPollingForTest();
  }

  private static boolean isConnectionFailure(SQLException e) {
    String state = e.getSQLState();
    return state != null && (state.startsWith("08") || state.startsWith("57"));
  }

  private static boolean isTransientState(SQLException e) {
    String state = e.getSQLState();
    return state != null && (state.startsWith("40") || state.equals("53300"));
  }

  private static void waitForRecovery(int attempt, long baseDelay) {
    try {
      // Exponential backoff: 1x, 2x, 4x the base delay
      long sleepTime = (long) (baseDelay * Math.pow(2, attempt - 1));
      Thread.sleep(sleepTime);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  @FunctionalInterface
  interface SqlRunnable {
    void run() throws SQLException;
  }

  private void dbRetry(SqlRunnable runnable) {
    dbRetry(
        () -> {
          runnable.run();
          return null;
        });
  }

  @FunctionalInterface
  interface SqlSupplier<T> {
    T get() throws SQLException;
  }

  private <T> T dbRetry(SqlSupplier<T> supplier) {
    final int MAX_RETRIES = 20;
    int attempt = 0;
    while (true) {
      try {
        return supplier.get();
      } catch (SQLException e) {
        if (++attempt > MAX_RETRIES) {
          String msg = "Database operation failed after %d attempts".formatted(attempt);
          throw new RuntimeException(msg, e);
        }
        if (e instanceof SQLRecoverableException || isConnectionFailure(e)) {
          logger.warn("Recoverable connection error. Resetting client pool.", e);
          if (dataSource instanceof HikariDataSource hikariDataSource) {
            hikariDataSource.getHikariPoolMXBean().softEvictConnections();
          }
          waitForRecovery(attempt, 2000);
        } else if (e instanceof SQLTransientException || isTransientState(e)) {
          logger.warn("Transient DB error. Retrying command.", e);
          waitForRecovery(attempt, 500);
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Initializes the status of a workflow.
   *
   * @param initStatus The initial workflow status details.
   * @param maxRetries Optional maximum number of retries.
   * @param isRecoveryRequest True if this is a recovery request, indicating that this node is told
   *     it owns the workflow even if the ID already exists
   * @param isDequeuedRequest True if this is a dequeue request, indicating that this node is told
   *     it owns the workflow (provided it is in the enqueued state)
   * @return An object containing the current status and optionally the deadline epoch milliseconds.
   * @throws DBOSConflictingWorkflowException If a conflicting workflow already exists.
   * @throws DBOSMaxRecoveryAttemptsExceededException If the workflow exceeds max retries.
   */
  public WorkflowInitResult initWorkflowStatus(
      WorkflowStatusInternal initStatus,
      Integer maxRetries,
      boolean isRecoveryRequest,
      boolean isDequeuedRequest) {

    // This ID will be used to tell if we are the first writer of the record, or if
    // there is an existing one.
    // Note that it is generated outside of the DB retry loop, in case commit acks
    // get lost and we do not know if we committed or not
    String ownerXid = UUID.randomUUID().toString();
    return dbRetry(
        () ->
            workflowDAO.initWorkflowStatus(
                initStatus, maxRetries, isRecoveryRequest, isDequeuedRequest, ownerXid));
  }

  /**
   * Store the result to workflow_status
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   */
  public void recordWorkflowOutput(String workflowId, String result) {
    dbRetry(() -> workflowDAO.recordWorkflowOutput(workflowId, result));
  }

  /**
   * Store the error to workflow_status
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   */
  public void recordWorkflowError(String workflowId, String error) {
    dbRetry(() -> workflowDAO.recordWorkflowError(workflowId, error));
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return dbRetry(() -> workflowDAO.getWorkflowStatus(workflowId));
  }

  public String getWorkflowSerialization(String workflowId) {
    return dbRetry(() -> workflowDAO.getWorkflowSerialization(workflowId));
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return dbRetry(() -> workflowDAO.listWorkflows(input));
  }

  public List<GetPendingWorkflowsOutput> getPendingWorkflows(String executorId, String appVersion) {
    return dbRetry(() -> workflowDAO.getPendingWorkflows(executorId, appVersion));
  }

  public boolean clearQueueAssignment(String workflowId) {
    return dbRetry(() -> queuesDAO.clearQueueAssignment(workflowId));
  }

  public List<String> getQueuePartitions(String queueName) {
    return dbRetry(() -> queuesDAO.getQueuePartitions(queueName));
  }

  public StepResult checkStepExecutionTxn(String workflowId, int functionId, String functionName) {

    return dbRetry(
        () -> {
          try (Connection connection = dataSource.getConnection()) {
            return StepsDAO.checkStepExecutionTxn(
                workflowId, functionId, functionName, connection, this.schema);
          }
        });
  }

  public void recordStepResultTxn(StepResult result, long startTime) {
    var et = System.currentTimeMillis();
    dbRetry(() -> StepsDAO.recordStepResultTxn(dataSource, result, startTime, et, this.schema));
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) {
    return dbRetry(() -> stepsDAO.listWorkflowSteps(workflowId));
  }

  public <T> Result<T> awaitWorkflowResult(String workflowId) {
    return dbRetry(() -> workflowDAO.<T>awaitWorkflowResult(workflowId));
  }

  public List<String> getAndStartQueuedWorkflows(
      Queue queue, String executorId, String appVersion, String partitionKey) {
    return dbRetry(
        () -> queuesDAO.getAndStartQueuedWorkflows(queue, executorId, appVersion, partitionKey));
  }

  public void recordChildWorkflow(
      String parentId,
      String childId, // workflowId of the
      // child
      int functionId, // func id in the parent
      String functionName,
      long startTime) {
    dbRetry(
        () ->
            workflowDAO.recordChildWorkflow(
                parentId, childId, functionId, functionName, startTime));
  }

  public Optional<String> checkChildWorkflow(String workflowUuid, int functionId) {
    return dbRetry(() -> workflowDAO.checkChildWorkflow(workflowUuid, functionId));
  }

  public void send(
      String workflowId,
      int stepId,
      String destinationId,
      Object message,
      String topic,
      String messageId,
      String serialization) {
    dbRetry(
        () ->
            notificationsDAO.send(
                workflowId, stepId, destinationId, message, topic, messageId, serialization));
  }

  public void sendDirect(
      String destinationId, Object message, String topic, String messageId, String serialization) {
    dbRetry(
        () -> notificationsDAO.sendDirect(destinationId, message, topic, messageId, serialization));
  }

  public Object recv(
      String workflowId, int stepId, int timeoutStepId, String topic, Duration timeout) {
    return dbRetry(() -> notificationsDAO.recv(workflowId, stepId, timeoutStepId, topic, timeout));
  }

  public void setEvent(
      String workflowId,
      int functionId,
      String key,
      Object message,
      boolean asStep,
      String serialization) {

    dbRetry(
        () ->
            notificationsDAO.setEvent(workflowId, functionId, key, message, asStep, serialization));
  }

  public Object getEvent(
      String targetId, String key, Duration timeout, GetWorkflowEventContext callerCtx) {

    return dbRetry(() -> notificationsDAO.getEvent(targetId, key, timeout, callerCtx));
  }

  public void sleep(String workflowId, int functionId, Duration duration) {
    dbRetry(() -> stepsDAO.sleep(workflowId, functionId, duration));
  }

  public void cancelWorkflows(List<String> workflowIds) {
    dbRetry(() -> workflowDAO.cancelWorkflows(workflowIds));
  }

  public void resumeWorkflows(List<String> workflowIds) {
    dbRetry(() -> workflowDAO.resumeWorkflows(workflowIds));
  }

  public void deleteWorkflows(List<String> workflowIds, boolean deleteChildren) {
    dbRetry(() -> workflowDAO.deleteWorkflows(workflowIds, deleteChildren));
  }

  public String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options) {
    return dbRetry(() -> workflowDAO.forkWorkflow(originalWorkflowId, startStep, options));
  }

  public void createApplicationVersion(String versionName) {
    dbRetry(() -> applicationVersionDAO.createApplicationVersion(versionName));
  }

  public void updateApplicationVersionTimestamp(String versionName, Instant newTimestamp) {
    dbRetry(
        () -> applicationVersionDAO.updateApplicationVersionTimestamp(versionName, newTimestamp));
  }

  public List<VersionInfo> listApplicationVersions() {
    return dbRetry(() -> applicationVersionDAO.listApplicationVersions());
  }

  public VersionInfo getLatestApplicationVersion() {
    return dbRetry(() -> applicationVersionDAO.getLatestApplicationVersion());
  }

  public void garbageCollect(Long cutoffEpochTimestampMs, Long rowsThreshold) {
    dbRetry(() -> workflowDAO.garbageCollect(cutoffEpochTimestampMs, rowsThreshold));
  }

  public void createSchedule(WorkflowSchedule schedule) {
    dbRetry(() -> schedulesDAO.createSchedule(schedule));
  }

  public Optional<WorkflowSchedule> getSchedule(String name) {
    return dbRetry(() -> schedulesDAO.getSchedule(name));
  }

  public List<WorkflowSchedule> listSchedules(
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes) {
    return dbRetry(() -> schedulesDAO.listSchedules(statuses, workflowNames, scheduleNamePrefixes));
  }

  public void pauseSchedule(String name) {
    dbRetry(() -> schedulesDAO.pauseSchedule(name));
  }

  public void resumeSchedule(String name) {
    dbRetry(() -> schedulesDAO.resumeSchedule(name));
  }

  public void updateScheduleLastFiredAt(String name, Instant lastFiredAt) {
    dbRetry(() -> schedulesDAO.updateScheduleLastFiredAt(name, lastFiredAt));
  }

  public void deleteSchedule(String name) {
    dbRetry(() -> schedulesDAO.deleteSchedule(name));
  }

  public void applySchedules(List<WorkflowSchedule> schedules) {
    dbRetry(() -> schedulesDAO.applySchedules(schedules));
  }

  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return dbRetry(() -> externalStateDAO.getExternalState(service, workflowName, key));
  }

  public ExternalState upsertExternalState(ExternalState state) {
    return dbRetry(() -> externalStateDAO.upsertExternalState(state));
  }

  public List<MetricData> getMetrics(Instant startTime, Instant endTime) {
    return dbRetry(() -> workflowDAO.getMetrics(startTime, endTime));
  }

  public boolean patch(String workflowId, int functionId, String patchName) {
    return dbRetry(() -> stepsDAO.patch(workflowId, functionId, patchName));
  }

  public boolean deprecatePatch(String workflowId, int functionId, String patchName) {
    return dbRetry(() -> stepsDAO.deprecatePatch(workflowId, functionId, patchName));
  }

  public Set<String> getWorkflowChildren(String workflowId) {
    return dbRetry(() -> workflowDAO.getWorkflowChildren(workflowId));
  }

  public List<ExportedWorkflow> exportWorkflow(String workflowId, boolean exportChildren) {
    return dbRetry(() -> workflowDAO.exportWorkflow(workflowId, exportChildren));
  }

  public void importWorkflow(List<ExportedWorkflow> workflows) {
    dbRetry(() -> workflowDAO.importWorkflow(workflows, this.serializer));
  }
}
