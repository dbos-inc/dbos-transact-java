package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NotificationsDAO {

  private static final Logger logger = LoggerFactory.getLogger(NotificationsDAO.class);

  private final DataSource dataSource;
  private final String schema;
  private final DBOSSerializer serializer;
  private NotificationService notificationService;
  private long dbPollingIntervalEventMs = 10000;

  NotificationsDAO(
      DataSource ds, NotificationService nService, String schema, DBOSSerializer serializer) {
    this.dataSource = ds;
    this.schema = Objects.requireNonNull(schema);
    this.notificationService = nService;
    this.serializer = serializer;
  }

  void speedUpPollingForTest() {
    dbPollingIntervalEventMs = 100;
  }

  void send(
      String workflowId,
      int stepId,
      String destinationId,
      Object message,
      String topic,
      String messageId,
      String serialization)
      throws SQLException {

    serialization =
        Objects.requireNonNullElse(serialization, SerializationStrategy.DEFAULT.formatName());
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.send";
    String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC;

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);

      try {
        // Check if operation was already executed
        StepResult recordedOutput =
            StepsDAO.checkStepExecutionTxn(workflowId, stepId, functionName, conn, this.schema);

        if (recordedOutput != null) {
          logger.debug(
              "Replaying send, id: {}, destination_uuid: {}, topic: {}",
              stepId,
              destinationId,
              finalTopic);
          conn.commit();
          return;
        } else {
          logger.debug(
              "Running send, id: {}, destination_uuid: {}, topic: {}",
              stepId,
              destinationId,
              finalTopic);
        }

        var finalMessageId = (messageId != null) ? messageId : UUID.randomUUID().toString();
        var serializedMsg =
            SerializationUtil.serializeValue(message, serialization, this.serializer);

        // Insert notification with serialization format
        final String sql =
            """
              INSERT INTO "%s".notifications
                (destination_uuid, topic, message, serialization, message_uuid)
              VALUES (?, ?, ?, ?, ?)
              ON CONFLICT (message_uuid) DO NOTHING
            """
                .formatted(this.schema);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          stmt.setString(1, destinationId);
          stmt.setString(2, finalTopic);
          stmt.setString(3, serializedMsg.serializedValue());
          stmt.setString(4, serializedMsg.serialization());
          stmt.setString(5, finalMessageId);
          stmt.executeUpdate();
        } catch (SQLException e) {
          // Foreign key violation
          if ("23503".equals(e.getSQLState())) {
            throw new DBOSNonExistentWorkflowException(destinationId);
          }
          throw e;
        }

        // Record operation result
        var output = new StepResult(workflowId, stepId, functionName, null, null, null, null);
        StepsDAO.recordStepResultTxn(
            output, startTime, System.currentTimeMillis(), conn, this.schema);

        conn.commit();

      } catch (Exception e) {
        try {
          conn.rollback();
        } catch (SQLException rollbackEx) {
          e.addSuppressed(rollbackEx);
        }
        throw e;
      }
    }
  }

  void sendDirect(
      String destinationId, Object message, String topic, String messageId, String serialization)
      throws SQLException {
    String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC;
    String finalMessageId = (messageId != null) ? messageId : UUID.randomUUID().toString();
    var serializedMsg = SerializationUtil.serializeValue(message, serialization, this.serializer);

    final String sql =
        """
          INSERT INTO "%s".notifications
            (destination_uuid, topic, message, message_uuid, serialization)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT (message_uuid) DO NOTHING
        """
            .formatted(this.schema);

    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, destinationId);
      stmt.setString(2, finalTopic);
      stmt.setString(3, serializedMsg.serializedValue());
      stmt.setString(4, finalMessageId);
      stmt.setString(5, serializedMsg.serialization());
      stmt.executeUpdate();
    } catch (SQLException e) {
      // Foreign key violation
      if ("23503".equals(e.getSQLState())) {
        throw new DBOSNonExistentWorkflowException(destinationId);
      }
      throw e;
    }
  }

  Object recv(String workflowId, int stepId, int timeoutFunctionId, String topic, Duration timeout)
      throws SQLException {

    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.recv";
    String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC;

    // First, check for previous executions
    StepResult recordedOutput = null;
    try (Connection c = dataSource.getConnection()) {
      recordedOutput =
          StepsDAO.checkStepExecutionTxn(workflowId, stepId, functionName, c, this.schema);
    }

    if (recordedOutput != null) {
      logger.debug("Replaying recv, id: {}, topic: {}", stepId, finalTopic);
      if (recordedOutput.output() != null) {
        return SerializationUtil.deserializeValue(
            recordedOutput.output(), recordedOutput.serialization(), this.serializer);
      } else {
        throw new RuntimeException("No output recorded in the last recv");
      }
    } else {
      logger.debug("Running recv, wfid {}, id: {}, topic: {}", workflowId, stepId, finalTopic);
    }

    // Insert a condition to the notifications map
    String payload = workflowId + "::" + finalTopic;
    var lockPair = new NotificationService.LockConditionPair();

    // Timeout / deadline for the notification
    double actualTimeout = timeout.toMillis();
    var targetTime = System.currentTimeMillis() + actualTimeout;
    var checkedDBForSleep = false;

    try {
      lockPair.lock.lock();
      boolean success = notificationService.registerNotificationCondition(payload, lockPair);
      if (!success) {
        // if this happens, the workflow is executing concurrently
        throw new DBOSWorkflowExecutionConflictException(workflowId);
      }

      while (true) {
        // Check if the key is already in the database. If not, wait for the
        // notification
        boolean hasExistingNotification = false;
        try (Connection conn = dataSource.getConnection()) {
          final String sql =
              """
              SELECT topic FROM "%s".notifications
              WHERE destination_uuid = ? AND topic = ? AND consumed = FALSE
              """
                  .formatted(this.schema);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, workflowId);
            stmt.setString(2, finalTopic);
            try (ResultSet rs = stmt.executeQuery()) {
              hasExistingNotification = rs.next();
            }
          }
        }

        if (hasExistingNotification) break;

        var nowTime = System.currentTimeMillis();

        // Wait for the notification
        if (!checkedDBForSleep) {
          // Support OAOO sleep
          actualTimeout =
              StepsDAO.durableSleepDuration(
                      dataSource,
                      workflowId,
                      timeoutFunctionId,
                      timeout,
                      this.schema,
                      this.serializer)
                  .toMillis();
          checkedDBForSleep = true;
          targetTime = nowTime + actualTimeout;
        }
        if (nowTime >= targetTime) break;
        long timeoutMs = (long) (Math.min(targetTime - nowTime, dbPollingIntervalEventMs));

        try {
          lockPair.condition.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for message", e);
        }
      }
    } finally {
      lockPair.lock.unlock();
      notificationService.unregisterNotificationCondition(payload);
    }

    // Transactionally consume and return the oldest unconsumed message, or null if none.
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);

      try {
        // Find and mark consumed the oldest entry for this workflow+topic
        final String sql =
            """
            UPDATE "%1$s".notifications
            SET consumed = TRUE
            WHERE destination_uuid = ?
              AND topic = ?
              AND consumed = FALSE
              AND message_uuid = (
                SELECT message_uuid FROM "%1$s".notifications
                WHERE destination_uuid = ?
                  AND topic = ?
                  AND consumed = FALSE
                ORDER BY created_at_epoch_ms ASC
                LIMIT 1
              )
            RETURNING message, serialization
            """
                .formatted(this.schema);

        String serializedMessage = null;
        String serialization = null;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          // JDBC uses positional parameters (?), and each placeholder must be bound explicitly,
          // so we need to set the same values again for the nested SELECT
          stmt.setString(1, workflowId);
          stmt.setString(2, finalTopic);
          stmt.setString(3, workflowId);
          stmt.setString(4, finalTopic);

          try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
              serializedMessage = rs.getString("message");
              serialization = rs.getString("serialization");
            }
          }
        }

        var recvdMessage =
            SerializationUtil.deserializeValue(serializedMessage, serialization, this.serializer);

        // Record operation result
        StepResult output =
            new StepResult(
                workflowId, stepId, functionName, serializedMessage, null, null, serialization);
        StepsDAO.recordStepResultTxn(
            output, startTime, System.currentTimeMillis(), conn, this.schema);

        conn.commit();
        return recvdMessage;

      } catch (Exception e) {
        conn.rollback();
        throw e;
      }
    }
  }

  private void setEvent(
      Connection conn,
      String workflowId,
      int functionId,
      String key,
      String message,
      String serialization)
      throws SQLException {
    final String eventSql =
        """
          INSERT INTO "%s".workflow_events (workflow_uuid, key, value, serialization)
          VALUES (?, ?, ?, ?)
          ON CONFLICT (workflow_uuid, key)
          DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization
        """
            .formatted(this.schema);

    try (PreparedStatement stmt = conn.prepareStatement(eventSql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, key);
      stmt.setString(3, message);
      stmt.setString(4, serialization);
      stmt.executeUpdate();
    }

    final String eventHistorySql =
        """
          INSERT INTO "%s".workflow_events_history (workflow_uuid, function_id, key, value, serialization)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT (workflow_uuid, key, function_id)
          DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization
        """
            .formatted(this.schema);

    try (PreparedStatement stmt = conn.prepareStatement(eventHistorySql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, functionId);
      stmt.setString(3, key);
      stmt.setString(4, message);
      stmt.setString(5, serialization);
      stmt.executeUpdate();
    }
  }

  void setEvent(
      String workflowId,
      int functionId,
      String key,
      Object message,
      boolean asStep,
      String serialization)
      throws SQLException {

    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.setEvent";

    // Serialize the message using the specified format
    SerializationUtil.SerializedResult serializedResult =
        SerializationUtil.serializeValue(message, serialization, this.serializer);

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        if (asStep) {
          // check for a previous operation result
          var recordedOutput =
              StepsDAO.checkStepExecutionTxn(
                  workflowId, functionId, functionName, conn, this.schema);
          if (recordedOutput != null) {
            logger.debug(
                "Replaying setEvent, workflow: {}, step: {}, key: {}", workflowId, functionId, key);
            conn.commit();
            return; // Already sent before
          } else {
            logger.debug(
                "Running setEvent, workflow: {}, step: {}, key: {}", workflowId, functionId, key);
          }
        }

        this.setEvent(
            conn,
            workflowId,
            functionId,
            key,
            serializedResult.serializedValue(),
            serializedResult.serialization());

        if (asStep) {
          // Record the operation result
          StepResult output =
              new StepResult(workflowId, functionId, functionName, null, null, null, null);
          StepsDAO.recordStepResultTxn(
              output, startTime, System.currentTimeMillis(), conn, this.schema);
        }

        conn.commit();
      } catch (Exception e) {
        logger.error(
            "setEvent rollback, workflow: {} id: {}, key: {}", workflowId, functionId, key, e);
        conn.rollback();
        throw e;
      }
    }
  }

  Object getEvent(
      String targetUuid, String key, Duration timeout, GetWorkflowEventContext callerCtx)
      throws SQLException {

    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.getEvent";

    // Check for previous executions only if it's in a workflow
    if (callerCtx != null) {

      StepResult recordedOutput = null;

      try (Connection conn = dataSource.getConnection()) {
        recordedOutput =
            StepsDAO.checkStepExecutionTxn(
                callerCtx.workflowId(), callerCtx.functionId(), functionName, conn, this.schema);
      }

      if (recordedOutput != null) {
        logger.debug("Replaying getEvent, id: {}, key: {}", callerCtx.functionId(), key);
        if (recordedOutput.output() != null) {
          return SerializationUtil.deserializeValue(
              recordedOutput.output(), recordedOutput.serialization(), this.serializer);
        } else {
          throw new RuntimeException("No output recorded in the last getEvent");
        }
      } else {
        logger.debug("Running getEvent, id: {}, key: {}", callerCtx.functionId(), key);
      }
    }

    String payload = targetUuid + "::" + key;
    NotificationService.LockConditionPair lockConditionPair =
        notificationService.getOrCreateNotificationCondition(payload);

    lockConditionPair.lock.lock();
    try {
      // Check if the key is already in the database. If not, wait for the
      // notification.
      Object value = null;
      final String sql =
          """
            SELECT value, serialization FROM "%s".workflow_events WHERE workflow_uuid = ? AND key = ?
          """
              .formatted(this.schema);

      // Wait for the notification
      double actualTimeout =
          Objects.requireNonNull(timeout, "getEvent timeout cannot be null").toMillis();
      var targetTime = System.currentTimeMillis() + actualTimeout;
      var checkedDBForSleep = false;
      var hasExistingNotification = false;

      while (true) {
        // Database check

        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {

          stmt.setString(1, targetUuid);
          stmt.setString(2, key);

          try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
              String serializedValue = rs.getString("value");
              String serialization = rs.getString("serialization");
              value =
                  SerializationUtil.deserializeValue(
                      serializedValue, serialization, this.serializer);
              hasExistingNotification = true;
            }
          }
        }

        if (hasExistingNotification) break;
        var nowTime = System.currentTimeMillis();
        if (nowTime > targetTime) break;

        // Consult DB - part of timeout may have expired if sleep is durable.
        if (callerCtx != null & !checkedDBForSleep) {
          actualTimeout =
              StepsDAO.durableSleepDuration(
                      dataSource,
                      callerCtx.workflowId(),
                      callerCtx.timeoutFunctionId(),
                      timeout,
                      this.schema,
                      this.serializer)
                  .toMillis();
          targetTime = System.currentTimeMillis() + actualTimeout;
          checkedDBForSleep = true;
          if (nowTime > targetTime) break;
        }

        try {
          long timeoutms = (long) (targetTime - nowTime);
          logger.debug("Waiting for notification {}...", timeout);
          lockConditionPair.condition.await(
              Math.min(timeoutms, dbPollingIntervalEventMs), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for event", e);
        }
      }

      // Record the output if it's in a workflow
      if (callerCtx != null) {
        var toSaveSer = SerializationUtil.serializeValue(value, null, this.serializer);
        StepResult output =
            new StepResult(
                    callerCtx.workflowId(),
                    callerCtx.functionId(),
                    functionName,
                    null,
                    null,
                    null,
                    toSaveSer.serialization())
                .withOutput(toSaveSer.serializedValue());
        StepsDAO.recordStepResultTxn(
            dataSource, output, startTime, System.currentTimeMillis(), this.schema);
      }

      return value;

    } finally {
      lockConditionPair.lock.unlock();
      // Remove the condition from the map after use
      notificationService.unregisterNotificationCondition(payload);
    }
  }
}
