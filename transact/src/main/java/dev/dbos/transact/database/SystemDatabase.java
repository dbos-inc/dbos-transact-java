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

  private final WorkflowDAO workflowDAO;
  private final StepsDAO stepsDAO;
  private final QueuesDAO queuesDAO;
  private final NotificationsDAO notificationsDAO;
  private final NotificationService notificationService;

  public SystemDatabase(DBOSConfig config) {
    this(SystemDatabase.createDataSource(config), Objects.requireNonNull(config).databaseSchema());
  }

  public SystemDatabase(HikariDataSource dataSource, String schema) {
    this.schema = sanitizeSchema(schema);
    this.dataSource = dataSource;
    stepsDAO = new StepsDAO(dataSource, this.schema);
    workflowDAO = new WorkflowDAO(dataSource, this.schema);
    queuesDAO = new QueuesDAO(dataSource, this.schema);
    notificationService = new NotificationService(dataSource);
    notificationsDAO = new NotificationsDAO(dataSource, notificationService, this.schema);
  }

  HikariConfig getConfig() {
    return dataSource;
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

  void speedUpPollingForTest() {
    workflowDAO.speedUpPollingForTest();
    notificationsDAO.speedUpPollingForTest();
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
    return DbRetry.call(
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
    DbRetry.run(
        () -> {
          workflowDAO.recordWorkflowOutput(workflowId, result);
        });
  }

  /**
   * Store the error to workflow_status
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   */
  public void recordWorkflowError(String workflowId, String error) {
    DbRetry.run(
        () -> {
          workflowDAO.recordWorkflowError(workflowId, error);
        });
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return DbRetry.call(
        () -> {
          return workflowDAO.getWorkflowStatus(workflowId);
        });
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

  public List<String> getQueuePartitions(String queueName) {
    return DbRetry.call(
        () -> {
          return queuesDAO.getQueuePartitions(queueName);
        });
  }

  public StepResult checkStepExecutionTxn(String workflowId, int functionId, String functionName) {

    return DbRetry.call(
        () -> {
          try (Connection connection = dataSource.getConnection()) {
            return StepsDAO.checkStepExecutionTxn(
                workflowId, functionId, functionName, connection, this.schema);
          }
        });
  }

  public void recordStepResultTxn(StepResult result, long startTime) {
    var et = System.currentTimeMillis();
    DbRetry.run(
        () -> {
          StepsDAO.recordStepResultTxn(dataSource, result, startTime, et, this.schema);
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
      Queue queue, String executorId, String appVersion, String partitionKey) {
    return DbRetry.call(
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
    DbRetry.run(
        () -> {
          workflowDAO.recordChildWorkflow(parentId, childId, functionId, functionName, startTime);
        });
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
      String workflowId, int functionId, int timeoutFunctionId, String topic, Duration timeout) {

    return DbRetry.call(
        () -> {
          try {
            return notificationsDAO.recv(workflowId, functionId, timeoutFunctionId, topic, timeout);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.error("recv() was interrupted", ie);
            throw new RuntimeException(ie.getMessage(), ie);
          }
        });
  }

  public void setEvent(
      String workflowId, int functionId, String key, Object message, boolean asStep) {

    DbRetry.run(
        () -> {
          notificationsDAO.setEvent(workflowId, functionId, key, message, asStep);
        });
  }

  public Object getEvent(
      String targetId, String key, Duration timeout, GetWorkflowEventContext callerCtx) {

    return DbRetry.call(
        () -> {
          return notificationsDAO.getEvent(targetId, key, timeout, callerCtx);
        });
  }

  public Duration sleep(String workflowId, int functionId, Duration duration, boolean skipSleep) {
    return DbRetry.call(
        () -> {
          return stepsDAO.sleep(workflowId, functionId, duration, skipSleep);
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

  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return DbRetry.call(
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
    return DbRetry.call(
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
    return DbRetry.call(
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
    return DbRetry.call(
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
    return DbRetry.call(
        () -> {
          try (Connection conn = dataSource.getConnection()) {
            var checkpointName = getCheckpointName(conn, workflowId, functionId);
            return patchName.equals(checkpointName);
          }
        });
  }

  // package public helper for test purposes
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
    if (config.dataSource() != null) {
      return config.dataSource();
    }

    var dburl = config.databaseUrl();
    var dbUser = config.dbUser();
    var dbPassword = config.dbPassword();
    var maximumPoolSize = config.maximumPoolSize();
    var connectionTimeout = config.connectionTimeout();

    return createDataSource(dburl, dbUser, dbPassword, maximumPoolSize, connectionTimeout);
  }
}
