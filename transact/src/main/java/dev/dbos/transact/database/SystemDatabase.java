package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowEvent;
import dev.dbos.transact.workflow.WorkflowEventHistory;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.WorkflowStream;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import javax.sql.DataSource;

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

  private final DataSource dataSource;
  private final String schema;
  private final boolean created;
  private final DBOSSerializer serializer;

  private final WorkflowDAO workflowDAO;
  private final StepsDAO stepsDAO;
  private final QueuesDAO queuesDAO;
  private final NotificationsDAO notificationsDAO;
  private final NotificationService notificationService;

  private SystemDatabase(
      DataSource dataSource, String schema, boolean created, DBOSSerializer serializer) {
    this.schema = sanitizeSchema(schema);
    this.dataSource = dataSource;
    this.created = created;
    this.serializer = serializer;

    stepsDAO = new StepsDAO(dataSource, this.schema, serializer);
    workflowDAO = new WorkflowDAO(dataSource, this.schema, serializer);
    queuesDAO = new QueuesDAO(dataSource, this.schema);
    notificationService = new NotificationService(dataSource);
    notificationsDAO =
        new NotificationsDAO(dataSource, notificationService, this.schema, serializer);
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

  public String getWorkflowSerialization(String workflowId) {
    return dbRetry(
        () -> {
          return workflowDAO.getWorkflowSerialization(workflowId);
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
      String workflowId,
      int functionId,
      String destinationId,
      Object message,
      String topic,
      String serialization) {

    dbRetry(
        () -> {
          notificationsDAO.send(
              workflowId, functionId, destinationId, message, topic, serialization);
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
      String workflowId,
      int functionId,
      String key,
      Object message,
      boolean asStep,
      String serialization) {

    dbRetry(
        () -> {
          notificationsDAO.setEvent(workflowId, functionId, key, message, asStep, serialization);
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
              var output =
                  new StepResult(workflowId, functionId, patchName, null, null, null, null);
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

  public void deleteWorkflows(String... workflowIds) {
    if (workflowIds == null || workflowIds.length == 0) {
      return;
    }

    var sql =
        """
          DELETE FROM %s.workflow_status
          WHERE workflow_uuid = ANY(?);
        """
            .formatted(this.schema);

    dbRetry(
        () -> {
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareStatement(sql)) {
            var array = conn.createArrayOf("text", workflowIds);
            stmt.setArray(1, array);
            stmt.executeUpdate();
          }
          return null;
        });
  }

  public List<String> getWorkflowChildren(String workflowId) {
    return dbRetry(() -> getWorkflowChildrenInternal(workflowId));
  }

  List<String> getWorkflowChildrenInternal(String workflowId) throws SQLException {
    var children = new HashSet<String>();
    var toProcess = new ArrayDeque<String>();
    toProcess.add(workflowId);

    var sql =
        """
          SELECT child_workflow_id
          FROM %s.operation_outputs
          WHERE workflow_uuid = ? AND child_workflow_id IS NOT NULL
        """
            .formatted(this.schema);

    try (var conn = dataSource.getConnection()) {
      while (!toProcess.isEmpty()) {
        var wfid = toProcess.poll();

        try (var stmt = conn.prepareStatement(sql)) {
          stmt.setString(1, wfid);

          try (var rs = stmt.executeQuery()) {
            while (rs.next()) {
              var childWorkflowId = rs.getString(1);
              if (!children.contains(childWorkflowId)) {
                children.add(childWorkflowId);
                toProcess.add(childWorkflowId);
              }
            }
          }
        }
      }
    }
    return new ArrayList<String>(children);
  }

  List<WorkflowEvent> listWorkflowEvents(Connection conn, String workflowId) throws SQLException {
    var sql =
        """
        SELECT key, value, serialization
        FROM %s.workflow_events
        WHERE workflow_uuid = ?
        """
            .formatted(this.schema);

    var events = new ArrayList<WorkflowEvent>();
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          var key = rs.getString("key");
          var value = rs.getString("value");
          var serialization = rs.getString("serialization");
          events.add(new WorkflowEvent(key, value, serialization));
        }
      }
    }
    return events;
  }

  List<WorkflowEventHistory> listWorkflowEventHistory(Connection conn, String workflowId)
      throws SQLException {
    var sql =
        """
        SELECT key, value, function_id, serialization
        FROM %s.workflow_events_history
        WHERE workflow_uuid = ?
        """
            .formatted(this.schema);

    var history = new ArrayList<WorkflowEventHistory>();
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          var key = rs.getString("key");
          var value = rs.getString("value");
          var stepId = rs.getInt("function_id");
          var serialization = rs.getString("serialization");
          history.add(new WorkflowEventHistory(key, value, stepId, serialization));
        }
      }
    }
    return history;
  }

  List<WorkflowStream> listWorkflowStreams(Connection conn, String workflowId) throws SQLException {
    var sql =
        """
        SELECT key, value, "offset", function_id, serialization
        FROM %s.streams
        WHERE workflow_uuid = ?
        """
            .formatted(this.schema);

    var streams = new ArrayList<WorkflowStream>();
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          var key = rs.getString("key");
          var value = rs.getString("value");
          var offset = rs.getInt("offset");
          var stepId = rs.getInt("function_id");
          var serialization = rs.getString("serialization");
          streams.add(new WorkflowStream(key, value, offset, stepId, serialization));
        }
      }
    }
    return streams;
  }

  public List<ExportedWorkflow> exportWorkflow(String workflowId, boolean exportChildren) {
    return dbRetry(
        () -> {
          var workflowIds =
              exportChildren
                  ? Stream.concat(
                          getWorkflowChildrenInternal(workflowId).stream(),
                          List.of(workflowId).stream())
                      .toList()
                  : List.of(workflowId);

          var workflows = new ArrayList<ExportedWorkflow>();
          for (var wfid : workflowIds) {
            try (var conn = dataSource.getConnection()) {
              var status = workflowDAO.getWorkflowStatus(conn, wfid);
              var steps = stepsDAO.listWorkflowSteps(conn, wfid);
              var events = listWorkflowEvents(conn, wfid);
              var eventHistory = listWorkflowEventHistory(conn, wfid);
              var streams = listWorkflowStreams(conn, wfid);
              workflows.add(new ExportedWorkflow(status, steps, events, eventHistory, streams));
            }
          }
          return workflows;
        });
  }

  public void importWorkflow(List<ExportedWorkflow> workflows) {
    var wfSQL =
        """
        INSERT INTO %s.workflow_status (
          workflow_uuid, status,
          name, class_name, config_name,
          authenticated_user, assumed_role, authenticated_roles,
          output, error, inputs,
          executor_id, application_version, application_id,
          created_at, updated_at, started_at_epoch_ms,
          queue_name, deduplication_id, priority, queue_partition_key,
          workflow_timeout_ms, workflow_deadline_epoch_ms,
          recovery_attempts, forked_from, parent_workflow_id, serialization
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
            .formatted(this.schema);

    var stepSQL =
        """
        INSERT INTO %s.operation_outputs (
          workflow_uuid, function_id, function_name,
          output, error, child_workflow_id,
          started_at_epoch_ms, completed_at_epoch_ms,
          serialization
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
            .formatted(this.schema);

    var eventSQL =
        """
        INSERT INTO %s.workflow_events (
          workflow_uuid, key, value, serialization
        ) VALUES (
          ?, ?, ?, ?
        )
        """
            .formatted(this.schema);

    var eventHistorySQL =
        """
        INSERT INTO %s.workflow_events_history (
          workflow_uuid, key, value, function_id, serialization
        ) VALUES (
          ?, ?, ?, ?, ?
        )
        """
            .formatted(this.schema);

    var streamsSQL =
        """
        INSERT INTO %s.streams (
          workflow_uuid, key, value, function_id, offset, serialization
        ) VALUES (
          ?, ?, ?, ?, ?, ?
        )
        """
            .formatted(this.schema);

    dbRetry(
        () -> {
          try (var conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
              for (var workflow : workflows) {

                var status = workflow.status();
                try (var stmt = conn.prepareStatement(wfSQL)) {
                  stmt.setString(1, status.workflowId());
                  stmt.setString(2, status.status().toString());
                  stmt.setString(3, status.name());
                  stmt.setString(4, status.className());
                  stmt.setString(5, status.instanceName());
                  stmt.setString(6, status.authenticatedUser());
                  stmt.setString(7, status.assumedRole());
                  stmt.setString(
                      8,
                      status.authenticatedRoles() == null
                          ? null
                          : JSONUtil.serializeArray(status.authenticatedRoles()));
                  stmt.setString(
                      9,
                      status.output() == null
                          ? null
                          : SerializationUtil.serializeValue(
                                  status.output(), status.serialization(), this.serializer)
                              .serializedValue());
                  stmt.setString(
                      10,
                      status.error() == null
                          ? null
                          : SerializationUtil.serializeError(
                                  status.error().throwable(),
                                  status.serialization(),
                                  this.serializer)
                              .serializedValue());
                  stmt.setString(
                      11,
                      status.input() == null
                          ? null
                          : SerializationUtil.serializeArgs(
                                  status.input(), null, status.serialization(), this.serializer)
                              .serializedValue());
                  stmt.setString(12, status.executorId());
                  stmt.setString(13, status.appVersion());
                  stmt.setString(14, status.appId());
                  stmt.setObject(15, status.createdAt());
                  stmt.setObject(16, status.updatedAt());
                  stmt.setObject(17, status.startedAtEpochMs());
                  stmt.setString(18, status.queueName());
                  stmt.setString(19, status.deduplicationId());
                  stmt.setObject(20, status.priority());
                  stmt.setString(21, status.queuePartitionKey());
                  stmt.setObject(22, status.timeoutMs());
                  stmt.setObject(23, status.deadlineEpochMs());
                  stmt.setObject(24, status.recoveryAttempts());
                  stmt.setString(25, status.forkedFrom());
                  stmt.setString(26, status.parentWorkflowId());
                  stmt.setString(27, status.serialization());

                  stmt.executeUpdate();
                }

                for (var step : workflow.steps()) {
                  try (var stmt = conn.prepareStatement(stepSQL)) {
                    stmt.setString(1, status.workflowId());
                    stmt.setInt(2, step.functionId());
                    stmt.setString(3, step.functionName());
                    stmt.setString(
                        4,
                        step.output() == null
                            ? null
                            : SerializationUtil.serializeValue(
                                    step.output(), step.serialization(), this.serializer)
                                .serializedValue());
                    stmt.setString(5, step.error() == null ? null : step.error().serializedError());
                    stmt.setString(6, step.childWorkflowId());
                    stmt.setObject(7, step.startedAtEpochMs());
                    stmt.setObject(8, step.completedAtEpochMs());
                    stmt.setString(9, step.serialization());

                    stmt.executeUpdate();
                  }
                }

                for (var event : workflow.events()) {
                  try (var stmt = conn.prepareStatement(eventSQL)) {
                    stmt.setString(1, status.workflowId());
                    stmt.setString(2, event.key());
                    stmt.setString(3, event.value());
                    stmt.setString(4, event.serialization());

                    stmt.executeUpdate();
                  }
                }

                for (var history : workflow.eventHistory()) {
                  try (var stmt = conn.prepareStatement(eventHistorySQL)) {
                    stmt.setString(1, status.workflowId());
                    stmt.setString(2, history.key());
                    stmt.setString(3, history.value());
                    stmt.setInt(4, history.stepId());
                    stmt.setString(5, history.serialization());

                    stmt.executeUpdate();
                  }
                }

                for (var stream : workflow.streams()) {
                  try (var stmt = conn.prepareStatement(streamsSQL)) {
                    stmt.setString(1, status.workflowId());
                    stmt.setString(2, stream.key());
                    stmt.setString(3, stream.value());
                    stmt.setInt(4, stream.stepId());
                    stmt.setInt(5, stream.offset());
                    stmt.setString(6, stream.serialization());

                    stmt.executeUpdate();
                  }
                }
              }
              conn.commit();
            } catch (SQLException e) {
              conn.rollback();
              throw e;
            }
          }

          return null;
        });
  }
}
