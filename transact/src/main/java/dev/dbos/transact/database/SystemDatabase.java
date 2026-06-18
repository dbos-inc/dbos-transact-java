package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.ApplicationVersionDAO;
import dev.dbos.transact.database.dao.ExternalStateDAO;
import dev.dbos.transact.database.dao.NotificationsDAO;
import dev.dbos.transact.database.dao.QueuesDAO;
import dev.dbos.transact.database.dao.SchedulesDAO;
import dev.dbos.transact.database.dao.StepsDAO;
import dev.dbos.transact.database.dao.StreamsDAO;
import dev.dbos.transact.database.dao.WorkflowDAO;
import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.database.signal.SignalMap;
import dev.dbos.transact.database.signal.Subscription;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.internal.Validation;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkFromFailureOptions;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.GetStepAggregatesInput;
import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.NotificationInfo;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.SendMessage;
import dev.dbos.transact.workflow.StepAggregateRow;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowAggregateRow;
import dev.dbos.transact.workflow.WorkflowDelay;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemDatabase implements AutoCloseable {

  public static final Object END_OF_STREAM = new Object();

  public interface NotificationSource {
    void start();

    void close();
  }

  class NullNotificationSource implements NotificationSource {

    @Override
    public void start() {}

    @Override
    public void close() {}
  }

  private static final Logger logger = LoggerFactory.getLogger(SystemDatabase.class);

  public static String sanitizeSchema(String schema) {
    return Objects.requireNonNullElse(schema, Constants.DB_SCHEMA).replace("\0", "");
  }

  private final DbContext ctx;
  private final boolean created;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final SignalMap signalMap = new SignalMap();
  private final Function<SignalKey, Subscription> createSubscription =
      key -> signalMap.subscribe(key.toString());
  private final NotificationSource notificationSource;
  private final Duration dbPollingInterval = Duration.ofSeconds(1);

  private static void validatePostgresDataSource(DataSource dataSource) {
    try (Connection conn = dataSource.getConnection()) {
      String productName = conn.getMetaData().getDatabaseProductName();
      if (!productName.toLowerCase().contains("postgresql")) {
        throw new IllegalStateException(
            "DBOS requires a PostgreSQL datasource, but the provided datasource reports: "
                + productName);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to validate DBOS datasource", e);
    }
  }

  private SystemDatabase(
      DataSource dataSource,
      String schema,
      boolean created,
      DBOSSerializer serializer,
      boolean useListenNotify) {
    validatePostgresDataSource(dataSource);
    schema = sanitizeSchema(schema);
    if (schema.contains("\"")) {
      throw new IllegalArgumentException("Schema name must not contain double quotes");
    }

    this.ctx = new DbContext(dataSource, schema, serializer, this.closed::get);
    this.created = created;
    try {
      useListenNotify = isCockroach(dataSource) ? false : useListenNotify;
    } catch (SQLException e) {
      logger.error("Failed to determine if dataSource is CockroachDB", e);
      useListenNotify = false;
    }

    notificationSource =
        useListenNotify
            ? new NotificationListenerSource(dataSource, signalMap::raiseSignal)
            : new NullNotificationSource();
  }

  public SystemDatabase(
      String url,
      String user,
      String password,
      String schema,
      DBOSSerializer serializer,
      boolean useListenNotify) {
    this(createDataSource(url, user, password), schema, true, serializer, useListenNotify);
  }

  public SystemDatabase(String url, String user, String password, String schema) {
    this(createDataSource(url, user, password), schema, true, null, true);
  }

  public SystemDatabase(DataSource dataSource, String schema) {
    this(dataSource, schema, false, null, true);
  }

  public SystemDatabase(DataSource dataSource, String schema, DBOSSerializer serializer) {
    this(dataSource, schema, false, serializer, true);
  }

  public static SystemDatabase create(DBOSConfig config) {
    if (config.dataSource() == null) {
      return new SystemDatabase(
          config.databaseUrl(),
          config.dbUser(),
          config.dbPassword(),
          config.databaseSchema(),
          config.serializer(),
          config.useListenNotify());
    } else {
      return new SystemDatabase(config.dataSource(), config.databaseSchema(), config.serializer());
    }
  }

  Optional<HikariConfig> getConfig() {
    if (ctx.dataSource() instanceof HikariDataSource hds) {
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

  public static boolean isCockroach(DataSource dataSource) throws SQLException {
    try (var conn = dataSource.getConnection()) {
      return isCockroach(conn);
    }
  }

  public static boolean isCockroach(Connection conn) throws SQLException {
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT version()")) {
      if (rs.next()) {
        return rs.getString(1).toLowerCase().contains("cockroachdb");
      }
    }
    return false;
  }

  @Override
  public void close() {
    closed.set(true);
    notificationSource.close();
    if (created && ctx.dataSource() instanceof HikariDataSource hikariDataSource) {
      hikariDataSource.close();
    }
  }

  public void start() {
    notificationSource.start();
  }

  private static boolean isConnectionFailure(SQLException e) {
    String state = e.getSQLState();
    if (state != null && (state.startsWith("08") || state.startsWith("57"))) {
      return true;
    }
    // HikariCP and JDBC throw connection errors without a SQLSTATE (e.g. "Connection is closed").
    // Walk the cause chain so wrapped exceptions are also caught.
    for (Throwable t = e; t != null; t = t.getCause()) {
      String msg = t.getMessage();
      if (msg != null) {
        String lower = msg.toLowerCase();
        if (lower.contains("connection is closed")
            || lower.contains("connection is not available")
            || lower.contains("connection reset")
            || lower.contains("broken pipe")
            || lower.contains("socket closed")) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isTransientState(SQLException e) {
    String state = e.getSQLState();
    return state != null && (state.startsWith("40") || state.startsWith("53"));
  }

  private static void sleepWithJitter(double baseMs) {
    double jitter = 0.5 + ThreadLocalRandom.current().nextDouble(); // [0.5, 1.5)
    long sleepMs = (long) (baseMs * jitter);
    try {
      Thread.sleep(sleepMs);
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
    double backoffMs = 1000.0;
    final double maxBackoffMs = 60_000.0;
    int attempt = 0;
    while (true) {
      if (closed.get()) {
        throw new IllegalStateException("SystemDatabase is closed");
      }
      try {
        return supplier.get();
      } catch (SQLException e) {
        attempt++;
        if (e instanceof SQLRecoverableException || isConnectionFailure(e)) {
          logger.warn(
              "Recoverable connection error (attempt {}), resetting client pool", attempt, e);
          if (ctx.dataSource() instanceof HikariDataSource hikariDataSource) {
            hikariDataSource.getHikariPoolMXBean().softEvictConnections();
          }
        } else if (e instanceof SQLTransientException || isTransientState(e)) {
          logger.warn("Transient DB error (attempt {}), retrying", attempt, e);
        } else {
          throw new RuntimeException(e);
        }
        sleepWithJitter(backoffMs);
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      }
    }
  }

  public static Instant toInstant(Long epochMs) {
    return epochMs != null ? Instant.ofEpochMilli(epochMs) : null;
  }

  public static Duration toDuration(Long ms) {
    return ms != null ? Duration.ofMillis(ms) : null;
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
            WorkflowDAO.initWorkflowStatus(
                ctx, initStatus, maxRetries, isRecoveryRequest, isDequeuedRequest, ownerXid));
  }

  /**
   * Store the result to workflow_status
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   */
  public void recordWorkflowOutput(String workflowId, String result) {
    dbRetry(() -> WorkflowDAO.recordWorkflowOutput(ctx, workflowId, result));
  }

  /**
   * Store the error to workflow_status
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   */
  public void recordWorkflowError(String workflowId, String error) {
    dbRetry(() -> WorkflowDAO.recordWorkflowError(ctx, workflowId, error));
  }

  /**
   * Insert a workflow_status row already in the ERROR state, for a workflow that was never started.
   * See {@link WorkflowDAO#recordErrorForUnstartedWorkflow}.
   */
  public void recordErrorForUnstartedWorkflow(WorkflowStatusInternal initStatus, String error) {
    dbRetry(() -> WorkflowDAO.recordErrorForUnstartedWorkflow(ctx, initStatus, error));
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getWorkflowStatus(ctx, workflowId));
  }

  public String getWorkflowSerialization(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getWorkflowSerialization(ctx, workflowId));
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return dbRetry(() -> WorkflowDAO.listWorkflows(ctx, input));
  }

  public @Nullable String findWorkflowIdByDeduplicationId(
      String queueName, String deduplicationId) {
    return dbRetry(
        () -> WorkflowDAO.findWorkflowIdByDeduplicationId(ctx, queueName, deduplicationId));
  }

  public List<WorkflowAggregateRow> getWorkflowAggregates(GetWorkflowAggregatesInput input) {
    return dbRetry(() -> WorkflowDAO.getWorkflowAggregates(ctx, input));
  }

  public List<StepAggregateRow> getStepAggregates(GetStepAggregatesInput input) {
    return dbRetry(() -> WorkflowDAO.getStepAggregates(ctx, input));
  }

  public boolean clearQueueAssignment(String workflowId) {
    return dbRetry(() -> QueuesDAO.clearQueueAssignment(ctx, workflowId));
  }

  public List<String> getQueuePartitions(String queueName) {
    return dbRetry(() -> QueuesDAO.getQueuePartitions(ctx, queueName));
  }

  public boolean upsertQueue(String name, QueueOptions options, boolean updateExisting) {
    if (Constants.DBOS_INTERNAL_QUEUE.equals(name)) {
      throw new IllegalArgumentException(
          String.format("%s is a reserved queue name", Constants.DBOS_INTERNAL_QUEUE));
    }
    return dbRetry(() -> QueuesDAO.upsertQueue(ctx, name, options, updateExisting));
  }

  public void updateQueue(String name, QueueOptions update) {
    dbRetry(() -> QueuesDAO.updateQueue(ctx, name, update));
  }

  public Optional<Queue> findQueue(String name) {
    return dbRetry(() -> QueuesDAO.findQueue(ctx, name));
  }

  public List<Queue> listQueues() {
    return dbRetry(() -> QueuesDAO.listQueues(ctx));
  }

  public boolean deleteQueue(String name) {
    return dbRetry(() -> QueuesDAO.deleteQueue(ctx, name));
  }

  public StepResult checkStepResult(String workflowId, int functionId, String functionName) {

    return dbRetry(
        () -> {
          try (Connection connection = ctx.getConnection()) {
            return StepsDAO.checkStepResult(
                connection, ctx.schema(), workflowId, functionId, functionName);
          }
        });
  }

  public void recordStepResult(StepResult result, long startTime) {
    var et = System.currentTimeMillis();
    dbRetry(() -> StepsDAO.recordStepResult(ctx, result, startTime, et));
  }

  public List<StepInfo> listWorkflowSteps(
      String workflowId, Boolean loadOutput, Integer limit, Integer offset) {
    return dbRetry(() -> StepsDAO.listWorkflowSteps(ctx, workflowId, loadOutput, limit, offset));
  }

  public <T> Result<T> awaitWorkflowResult(String workflowId) {
    return dbRetry(() -> WorkflowDAO.<T>awaitWorkflowResult(ctx, dbPollingInterval, workflowId));
  }

  public List<String> startQueuedWorkflows(
      Queue queue,
      String executorId,
      String appVersion,
      String partitionKey,
      long localRunningCount) {
    return dbRetry(
        () ->
            QueuesDAO.startQueuedWorkflows(
                ctx, queue, executorId, appVersion, partitionKey, localRunningCount));
  }

  public void recordChildWorkflow(
      String parentId,
      String childId, // workflowId of the child
      int functionId, // func id in the parent
      String functionName,
      long startTime) {
    dbRetry(
        () ->
            WorkflowDAO.recordChildWorkflow(
                ctx, parentId, childId, functionId, functionName, startTime));
  }

  public Optional<String> checkChildWorkflow(String workflowUuid, int functionId) {
    return dbRetry(() -> WorkflowDAO.checkChildWorkflow(ctx, workflowUuid, functionId));
  }

  public void sendBulk(
      List<SendMessage> messages,
      String workflowId,
      int stepId,
      String functionName,
      boolean sendToForks,
      String serialization) {
    dbRetry(
        () ->
            NotificationsDAO.sendBulk(
                ctx, messages, workflowId, stepId, functionName, sendToForks, serialization));
  }

  public Object recv(
      String workflowId, int stepId, int timeoutStepId, String topic, Duration timeout) {
    return dbRetry(
        () ->
            NotificationsDAO.recv(
                ctx,
                workflowId,
                stepId,
                timeout,
                timeoutStepId,
                topic,
                dbPollingInterval,
                createSubscription));
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
            NotificationsDAO.setEvent(
                ctx, workflowId, functionId, key, message, asStep, serialization));
  }

  public Object getEvent(String targetId, String key, Duration timeout, GetEventCaller caller) {
    return dbRetry(
        () ->
            NotificationsDAO.getEvent(
                ctx, targetId, key, timeout, caller, dbPollingInterval, createSubscription));
  }

  public void sleep(String workflowId, int functionId, Duration duration) {
    dbRetry(() -> StepsDAO.sleep(ctx, workflowId, functionId, duration));
  }

  public void cancelWorkflows(List<String> workflowIds, boolean cancelChildren) {
    dbRetry(() -> WorkflowDAO.cancelWorkflows(ctx, workflowIds, cancelChildren));
  }

  public void resumeWorkflows(List<String> workflowIds, String queueName) {
    dbRetry(() -> WorkflowDAO.resumeWorkflows(ctx, workflowIds, queueName));
  }

  public void updateWorkflowAttributes(String workflowId, Map<String, Object> attributes) {
    var validated = Validation.validateAttributes(attributes);
    dbRetry(() -> WorkflowDAO.updateWorkflowAttributes(ctx, workflowId, validated));
  }

  public void deleteWorkflows(List<String> workflowIds, boolean deleteChildren) {
    dbRetry(() -> WorkflowDAO.deleteWorkflows(ctx, workflowIds, deleteChildren));
  }

  public String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options) {
    return dbRetry(() -> WorkflowDAO.forkWorkflow(ctx, originalWorkflowId, startStep, options));
  }

  public List<String> forkFromFailure(List<String> workflowIds, ForkFromFailureOptions options) {
    return dbRetry(() -> WorkflowDAO.forkFromFailure(ctx, workflowIds, options));
  }

  public void createApplicationVersion(String versionName) {
    dbRetry(() -> ApplicationVersionDAO.createApplicationVersion(ctx, versionName));
  }

  public void updateApplicationVersionTimestamp(String versionName, Instant newTimestamp) {
    dbRetry(
        () ->
            ApplicationVersionDAO.updateApplicationVersionTimestamp(
                ctx, versionName, newTimestamp));
  }

  public List<VersionInfo> listApplicationVersions() {
    return dbRetry(() -> ApplicationVersionDAO.listApplicationVersions(ctx));
  }

  public VersionInfo getLatestApplicationVersion() {
    return dbRetry(() -> ApplicationVersionDAO.getLatestApplicationVersion(ctx));
  }

  public void garbageCollect(Instant cutoff, Long rowsThreshold) {
    dbRetry(() -> WorkflowDAO.garbageCollect(ctx, cutoff, rowsThreshold));
  }

  public void setWorkflowDelay(String workflowId, WorkflowDelay delay) {
    dbRetry(() -> WorkflowDAO.setWorkflowDelay(ctx, workflowId, delay));
  }

  public void transitionDelayedWorkflows() {
    dbRetry(() -> WorkflowDAO.transitionDelayedWorkflows(ctx));
  }

  public void createSchedule(WorkflowSchedule schedule) {
    dbRetry(() -> SchedulesDAO.createSchedule(ctx, schedule));
  }

  public Optional<WorkflowSchedule> getSchedule(String name) {
    return dbRetry(() -> SchedulesDAO.getSchedule(ctx, name));
  }

  public List<WorkflowSchedule> listSchedules(
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes) {
    return dbRetry(
        () -> SchedulesDAO.listSchedules(ctx, statuses, workflowNames, scheduleNamePrefixes));
  }

  public void pauseSchedule(String name) {
    dbRetry(() -> SchedulesDAO.pauseSchedule(ctx, name));
  }

  public void resumeSchedule(String name) {
    dbRetry(() -> SchedulesDAO.resumeSchedule(ctx, name));
  }

  public void updateScheduleLastFiredAt(String name, Instant lastFiredAt) {
    dbRetry(() -> SchedulesDAO.updateScheduleLastFiredAt(ctx, name, lastFiredAt));
  }

  public void deleteSchedule(String name) {
    dbRetry(() -> SchedulesDAO.deleteSchedule(ctx, name));
  }

  public void applySchedules(List<WorkflowSchedule> schedules) {
    dbRetry(() -> SchedulesDAO.applySchedules(ctx, schedules));
  }

  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return dbRetry(() -> ExternalStateDAO.getExternalState(ctx, service, workflowName, key));
  }

  public ExternalState upsertExternalState(ExternalState state) {
    return dbRetry(() -> ExternalStateDAO.upsertExternalState(ctx, state));
  }

  public List<MetricData> getMetrics(Instant startTime, Instant endTime) {
    return dbRetry(() -> WorkflowDAO.getMetrics(ctx, startTime, endTime));
  }

  public boolean patch(String workflowId, int functionId, String patchName) {
    return dbRetry(() -> StepsDAO.patch(ctx, workflowId, functionId, patchName));
  }

  public boolean deprecatePatch(String workflowId, int functionId, String patchName) {
    return dbRetry(() -> StepsDAO.deprecatePatch(ctx, workflowId, functionId, patchName));
  }

  public Set<String> getWorkflowChildren(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getWorkflowChildren(ctx, workflowId));
  }

  public Map<String, Object> getAllEvents(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getAllEvents(ctx, workflowId));
  }

  public List<NotificationInfo> getAllNotifications(String workflowId) {
    return dbRetry(() -> NotificationsDAO.getAllNotifications(ctx, workflowId));
  }

  public List<ExportedWorkflow> exportWorkflow(String workflowId, boolean exportChildren) {
    return dbRetry(() -> WorkflowDAO.exportWorkflow(ctx, workflowId, exportChildren));
  }

  public void importWorkflow(List<ExportedWorkflow> workflows) {
    dbRetry(() -> WorkflowDAO.importWorkflow(ctx, workflows));
  }

  public void writeStreamFromStep(
      String workflowId, int functionId, String key, Object value, String serializationFormat) {
    dbRetry(
        () ->
            StreamsDAO.writeStreamFromStep(
                ctx, workflowId, functionId, key, value, serializationFormat));
  }

  public void writeStreamFromWorkflow(
      String workflowId, int functionId, String key, Object value, String serializationFormat) {
    dbRetry(
        () ->
            StreamsDAO.writeStreamFromWorkflow(
                ctx, workflowId, functionId, key, value, serializationFormat));
  }

  public void closeStream(String workflowId, int functionId, String key) {
    dbRetry(() -> StreamsDAO.closeStream(ctx, workflowId, functionId, key));
  }

  public Object readStream(String workflowId, String key, int offset) {
    return dbRetry(
        () ->
            StreamsDAO.readStream(
                ctx, workflowId, key, offset, dbPollingInterval, createSubscription));
  }

  public Map<String, List<Object>> getAllStreamEntries(String workflowId) {
    return dbRetry(() -> StreamsDAO.getAllStreamEntries(ctx, workflowId));
  }
}
