package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemDatabase implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SystemDatabase.class);

  public static String sanitizeSchema(String schema) {
    schema =
        Objects.requireNonNullElse(schema, Constants.DB_SCHEMA)
            .replace("\0", "")
            .replace("\"", "\"\"");
    return "\"%s\"".formatted(schema);
  }

  private final HikariDataSource dataSource;
  private final String schema;
  private final boolean created;

  private final WorkflowDAO workflowDAO;
  private final StepsDAO stepsDAO;
  private final QueuesDAO queuesDAO;
  private final NotificationsDAO notificationsDAO;
  private final NotificationService notificationService;

  private SystemDatabase(HikariDataSource dataSource, String schema, boolean created) {
    this.schema = sanitizeSchema(schema);
    this.dataSource = dataSource;
    this.created = created;

    stepsDAO = new StepsDAO(dataSource, this.schema);
    workflowDAO = new WorkflowDAO(dataSource, this.schema);
    queuesDAO = new QueuesDAO(dataSource, this.schema);
    notificationService = new NotificationService(dataSource);
    notificationsDAO = new NotificationsDAO(dataSource, notificationService, this.schema);
  }

  public SystemDatabase(String url, String user, String password, String schema) {
    this(createDataSource(url, user, password), schema, true);
  }

  public SystemDatabase(HikariDataSource dataSource, String schema) {
    this(dataSource, schema, false);
  }

  public static SystemDatabase create(DBOSConfig config) {
    if (config.dataSource() == null) {
      return new SystemDatabase(
          config.databaseUrl(), config.dbUser(), config.dbPassword(), config.databaseSchema());
    } else {
      return new SystemDatabase(config.dataSource(), config.databaseSchema());
    }
  }

  HikariConfig getConfig() {
    return dataSource;
  }

  Connection getSysDBConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public static HikariDataSource createDataSource(DBOSConfig config) {
    return createDataSource(config.databaseUrl(), config.dbUser(), config.dbPassword());
  }

  public static HikariDataSource createDataSource(String url, String user, String password) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(user);
    config.setPassword(password);

    // config.setMaxLifetime(60_000);
    // config.setKeepaliveTime(30000);
    // config.setConnectionTimeout(10000);
    // config.setValidationTimeout(2000);
    // config.setInitializationFailTimeout(-1);
    // config.setMaximumPoolSize(10);
    // config.setMinimumIdle(10);

    // config.addDataSourceProperty("tcpKeepAlive", "true");
    // config.addDataSourceProperty("connectTimeout", "10");
    // config.addDataSourceProperty("socketTimeout", "60");
    // config.addDataSourceProperty("reWriteBatchedInserts", "true");

    return new HikariDataSource(config);
  }

  @Override
  public void close() {
    notificationService.stop();
    if (created) {
      dataSource.close();
    }
  }

  public void start() {
    notificationService.start();
  }

  void speedUpPollingForTest() {
    workflowDAO.speedUpPollingForTest();
    notificationsDAO.speedUpPollingForTest();
  }

  @FunctionalInterface
  interface SqlSupplier<T> {
    T get() throws SQLException;
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
          dataSource.getHikariPoolMXBean().softEvictConnections();
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
        () -> {
          return workflowDAO.initWorkflowStatus(
              initStatus, maxRetries, isRecoveryRequest, isDequeuedRequest, ownerXid);
        });
  }

  /**
   * Store the result to workflow_status
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   */
  public void recordWorkflowOutput(String workflowId, String result) {
    dbRetry(
        () -> {
          workflowDAO.recordWorkflowOutput(workflowId, result);
          return null;
        });
  }

  /**
   * Store the error to workflow_status
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   */
  public void recordWorkflowError(String workflowId, String error) {
    dbRetry(
        () -> {
          workflowDAO.recordWorkflowError(workflowId, error);
          return null;
        });
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return dbRetry(
        () -> {
          return workflowDAO.getWorkflowStatus(workflowId);
        });
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return dbRetry(
        () -> {
          return workflowDAO.listWorkflows(input);
        });
  }

  public List<GetPendingWorkflowsOutput> getPendingWorkflows(String executorId, String appVersion) {
    return dbRetry(
        () -> {
          return workflowDAO.getPendingWorkflows(executorId, appVersion);
        });
  }

  public boolean clearQueueAssignment(String workflowId) {
    return dbRetry(
        () -> {
          return queuesDAO.clearQueueAssignment(workflowId);
        });
  }

  public List<String> getQueuePartitions(String queueName) {
    return dbRetry(
        () -> {
          return queuesDAO.getQueuePartitions(queueName);
        });
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
    dbRetry(
        () -> {
          StepsDAO.recordStepResultTxn(dataSource, result, startTime, et, this.schema);
          return null;
        });
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) {
    return dbRetry(
        () -> {
          return stepsDAO.listWorkflowSteps(workflowId);
        });
  }

  public <T> Result<T> awaitWorkflowResult(String workflowId) {
    return dbRetry(() -> workflowDAO.<T>awaitWorkflowResult(workflowId));
  }

  public List<String> getAndStartQueuedWorkflows(
      Queue queue, String executorId, String appVersion, String partitionKey) {
    return dbRetry(
        () -> {
          return queuesDAO.getAndStartQueuedWorkflows(queue, executorId, appVersion, partitionKey);
        });
  }

  public void recordChildWorkflow(
      String parentId,
      String childId, // workflowId of the
      // child
      int functionId, // func id in the parent
      String functionName,
      long startTime) {
    dbRetry(
        () -> {
          workflowDAO.recordChildWorkflow(parentId, childId, functionId, functionName, startTime);
          return null;
        });
  }

  public Optional<String> checkChildWorkflow(String workflowUuid, int functionId) {
    return dbRetry(
        () -> {
          return workflowDAO.checkChildWorkflow(workflowUuid, functionId);
        });
  }

  public void send(
      String workflowId, int functionId, String destinationId, Object message, String topic) {

    dbRetry(
        () -> {
          notificationsDAO.send(workflowId, functionId, destinationId, message, topic);
          return null;
        });
  }

  public Object recv(
      String workflowId, int functionId, int timeoutFunctionId, String topic, Duration timeout) {

    return dbRetry(
        () -> {
          return notificationsDAO.recv(workflowId, functionId, timeoutFunctionId, topic, timeout);
        });
  }

  public void setEvent(
      String workflowId, int functionId, String key, Object message, boolean asStep) {

    dbRetry(
        () -> {
          notificationsDAO.setEvent(workflowId, functionId, key, message, asStep);
          return null;
        });
  }

  public Object getEvent(
      String targetId, String key, Duration timeout, GetWorkflowEventContext callerCtx) {

    return dbRetry(
        () -> {
          return notificationsDAO.getEvent(targetId, key, timeout, callerCtx);
        });
  }

  public void sleep(String workflowId, int functionId, Duration duration) {
    dbRetry(
        () -> {
          stepsDAO.sleep(workflowId, functionId, duration);
          return null;
        });
  }

  public void cancelWorkflow(String workflowId) {
    dbRetry(
        () -> {
          workflowDAO.cancelWorkflow(workflowId);
          return null;
        });
  }

  public void resumeWorkflow(String workflowId) {
    dbRetry(
        () -> {
          workflowDAO.resumeWorkflow(workflowId);
          return null;
        });
  }

  public String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options) {
    return dbRetry(
        () -> {
          return workflowDAO.forkWorkflow(originalWorkflowId, startStep, options);
        });
  }

  public void garbageCollect(Long cutoffEpochTimestampMs, Long rowsThreshold) {
    dbRetry(
        () -> {
          workflowDAO.garbageCollect(cutoffEpochTimestampMs, rowsThreshold);
          return null;
        });
  }

  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return dbRetry(
        () -> {
          final String sql =
              """
                SELECT value, update_seq, update_time FROM %s.event_dispatch_kv WHERE service_name = ? AND workflow_fn_name = ? AND key = ?
              """
                  .formatted(this.schema);

          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, Objects.requireNonNull(service, "service must not be null"));
            stmt.setString(
                2, Objects.requireNonNull(workflowName, "workflowName must not be null"));
            stmt.setString(3, Objects.requireNonNull(key, "key must not be null"));

            try (var rs = stmt.executeQuery()) {
              if (rs.next()) {
                var value = rs.getString("value");
                BigDecimal seqDecimal = rs.getBigDecimal("update_seq");
                BigInteger seq = seqDecimal != null ? seqDecimal.toBigInteger() : null;
                BigDecimal time = rs.getBigDecimal("update_time");
                return Optional.of(new ExternalState(service, workflowName, key, value, time, seq));
              } else {
                return Optional.empty();
              }
            }
          }
        });
  }

  public ExternalState upsertExternalState(ExternalState state) {
    return dbRetry(
        () -> {
          final var sql =
              """
                INSERT INTO %s.event_dispatch_kv (
                service_name, workflow_fn_name, key, value, update_time, update_seq)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (service_name, workflow_fn_name, key)
                DO UPDATE SET
                  update_time = GREATEST(EXCLUDED.update_time, event_dispatch_kv.update_time),
                  update_seq =  GREATEST(EXCLUDED.update_seq,  event_dispatch_kv.update_seq),
                  value = CASE WHEN (EXCLUDED.update_time > event_dispatch_kv.update_time
                    OR EXCLUDED.update_seq > event_dispatch_kv.update_seq
                    OR (event_dispatch_kv.update_time IS NULL and event_dispatch_kv.update_seq IS NULL)
                  ) THEN EXCLUDED.value ELSE event_dispatch_kv.value END
                RETURNING value, update_time, update_seq
              """
                  .formatted(this.schema);

          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, Objects.requireNonNull(state.service(), "service must not be null"));
            stmt.setString(
                2, Objects.requireNonNull(state.workflowName(), "workflowName must not be null"));
            stmt.setString(3, Objects.requireNonNull(state.key(), "key must not be null"));
            stmt.setString(4, state.value());
            stmt.setObject(5, state.updateTime());
            stmt.setObject(6, state.updateSeq());

            try (var rs = stmt.executeQuery()) {
              if (rs.next()) {
                var value = rs.getString("value");
                BigDecimal seqDecimal = rs.getBigDecimal("update_seq");
                BigInteger seq = seqDecimal != null ? seqDecimal.toBigInteger() : null;
                BigDecimal time = rs.getBigDecimal("update_time");
                return new ExternalState(
                    state.service(), state.workflowName(), state.key(), value, time, seq);
              } else {
                throw new RuntimeException(
                    "Attempted to upsert external state %s / %s / %s"
                        .formatted(state.service(), state.workflowName(), state.key()));
              }
            }
          }
        });
  }

  public List<MetricData> getMetrics(Instant startTime, Instant endTime) {
    final var start = Objects.requireNonNull(startTime).toEpochMilli();
    final var end = Objects.requireNonNull(endTime).toEpochMilli();
    return dbRetry(
        () -> {
          logger.debug("getMetrics {} {}", start, end);
          List<MetricData> metrics = new ArrayList<>();
          final var wfSQL =
              """
                SELECT name, COUNT(workflow_uuid) as count
                FROM %s.workflow_status
                WHERE created_at >= ? AND created_at < ?
                GROUP BY name
              """
                  .formatted(this.schema);
          final var stepSQL =
              """
                SELECT function_name, COUNT(*) as count
                FROM %s.operation_outputs
                WHERE completed_at_epoch_ms >= ? AND completed_at_epoch_ms < ?
                GROUP BY function_name
              """
                  .formatted(this.schema);

          try (var conn = dataSource.getConnection();
              var ps1 = conn.prepareStatement(wfSQL);
              var ps2 = conn.prepareStatement(stepSQL)) {

            ps1.setLong(1, start);
            ps1.setLong(2, end);

            try (var rs = ps1.executeQuery()) {
              while (rs.next()) {
                var name = rs.getString("name");
                var count = rs.getInt("count");
                metrics.add(new MetricData("workflow_count", name, count));
              }
            }

            ps2.setLong(1, start);
            ps2.setLong(2, end);

            try (var rs = ps2.executeQuery()) {
              while (rs.next()) {
                var name = rs.getString("function_name");
                var count = rs.getInt("count");
                metrics.add(new MetricData("step_count", name, count));
              }
            }
          }

          return metrics;
        });
  }

  private String getCheckpointName(Connection conn, String workflowId, int functionId)
      throws SQLException {
    var sql =
        """
          SELECT function_name
          FROM %s.operation_outputs
          WHERE workflow_uuid = ? AND function_id = ?
        """
            .formatted(this.schema);

    try (var ps = conn.prepareStatement(sql)) {
      ps.setString(1, workflowId);
      ps.setInt(2, functionId);
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getString("function_name");
        } else {
          return null;
        }
      }
    }
  }

  public boolean patch(String workflowId, int functionId, String patchName) {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    return dbRetry(
        () -> {
          try (Connection conn = dataSource.getConnection()) {
            var checkpointName = getCheckpointName(conn, workflowId, functionId);
            if (checkpointName == null) {
              var output = new StepResult(workflowId, functionId, patchName);
              StepsDAO.recordStepResultTxn(
                  output, System.currentTimeMillis(), null, conn, this.schema);
              return true;
            } else {
              return patchName.equals(checkpointName);
            }
          }
        });
  }

  public boolean deprecatePatch(String workflowId, int functionId, String patchName) {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    return dbRetry(
        () -> {
          try (Connection conn = dataSource.getConnection()) {
            var checkpointName = getCheckpointName(conn, workflowId, functionId);
            return patchName.equals(checkpointName);
          }
        });
  }
}
