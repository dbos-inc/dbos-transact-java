package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NotificationsDAO {

  private static final Logger logger = LoggerFactory.getLogger(NotificationsDAO.class);

  private final HikariDataSource dataSource;
  private final String schema;
  private NotificationService notificationService;
  private long dbPollingIntervalEventMs = 10000;

  NotificationsDAO(HikariDataSource ds, NotificationService nService, String schema) {
    this.dataSource = ds;
    this.schema = Objects.requireNonNull(schema);
    this.notificationService = nService;
  }

  void speedUpPollingForTest() {
    dbPollingIntervalEventMs = 100;
  }

  void send(
      String workflowUuid, int functionId, String destinationUuid, Object message, String topic)
      throws SQLException {

    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.send";
    String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC;

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);

      try {
        // Check if operation was already executed
        StepResult recordedOutput =
            StepsDAO.checkStepExecutionTxn(
                workflowUuid, functionId, functionName, conn, this.schema);

        if (recordedOutput != null) {
          logger.debug(
              "Replaying send, id: {}, destination_uuid: {}, topic: {}",
              functionId,
              destinationUuid,
              finalTopic);
          conn.commit();
          return;
        } else {
          logger.debug(
              "Running send, id: {}, destination_uuid: {}, topic: {}",
              functionId,
              destinationUuid,
              finalTopic);
        }

        // Insert notification
        final String sql =
            """
              INSERT INTO %s.notifications (destination_uuid, topic, message) VALUES (?, ?, ?)
            """
                .formatted(this.schema);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          stmt.setString(1, destinationUuid);
          stmt.setString(2, finalTopic);
          stmt.setString(3, JSONUtil.serialize(message));
          stmt.executeUpdate();
        } catch (SQLException e) {
          // Foreign key violation
          if ("23503".equals(e.getSQLState())) {
            throw new DBOSNonExistentWorkflowException(destinationUuid);
          }
          throw e;
        }

        // Record operation result
        var output = new StepResult(workflowUuid, functionId, functionName);
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

  Object recv(
      String workflowUuid, int functionId, int timeoutFunctionId, String topic, Duration timeout)
      throws SQLException, InterruptedException {

    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.recv";
    String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC;

    // First, check for previous executions
    StepResult recordedOutput = null;

    try (Connection c = dataSource.getConnection()) {
      recordedOutput =
          StepsDAO.checkStepExecutionTxn(workflowUuid, functionId, functionName, c, this.schema);
    }

    if (recordedOutput != null) {
      logger.debug("Replaying recv, id: {}, topic: {}", functionId, finalTopic);
      if (recordedOutput.output() != null) {
        Object[] dSerOut = JSONUtil.deserializeToArray(recordedOutput.output());
        return dSerOut == null ? null : dSerOut[0];
      } else {
        throw new RuntimeException("No output recorded in the last recv");
      }
    } else {
      logger.debug(
          "Running recv, wfid {}, id: {}, topic: {}", workflowUuid, functionId, finalTopic);
    }

    // Insert a condition to the notifications map
    String payload = workflowUuid + "::" + finalTopic;
    NotificationService.LockConditionPair lockPair = new NotificationService.LockConditionPair();

    // Timeout / deadline for the notification
    double actualTimeout = timeout.toMillis();
    var targetTime = System.currentTimeMillis() + actualTimeout;
    var checkedDBForSleep = false;

    try {
      lockPair.lock.lock();
      boolean success = notificationService.registerNotificationCondition(payload, lockPair);
      if (!success) {
        // if this happens, the workflow is executing concurrently
        throw new DBOSWorkflowExecutionConflictException(workflowUuid);
      }

      while (true) {
        // Check if the key is already in the database. If not, wait for the
        // notification
        boolean hasExistingNotification = false;
        try (Connection conn = dataSource.getConnection()) {
          final String sql =
              """
                SELECT topic FROM %s.notifications WHERE destination_uuid = ? AND topic = ?
              """
                  .formatted(this.schema);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, workflowUuid);
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
              StepsDAO.sleep(
                      dataSource, workflowUuid, timeoutFunctionId, timeout, true, this.schema)
                  .toMillis();
          checkedDBForSleep = true;
          targetTime = nowTime + actualTimeout;
        }
        if (nowTime >= targetTime) break;
        long timeoutMs = (long) (Math.min(targetTime - nowTime, dbPollingIntervalEventMs));
        lockPair.condition.await(timeoutMs, TimeUnit.MILLISECONDS);
      }
    } finally {
      lockPair.lock.unlock();
      notificationService.unregisterNotificationCondition(payload);
    }

    // Transactionally consume and return the message if it's in the database
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);

      try {
        // Find and delete the oldest entry for this workflow+topic
        final String sql =
            """
              WITH oldest_entry AS (
                  SELECT destination_uuid, topic, message, created_at_epoch_ms
                  FROM %1$s.notifications
                  WHERE destination_uuid = ? AND topic = ?
                  ORDER BY created_at_epoch_ms ASC
                  LIMIT 1
              )
              DELETE FROM %1$s.notifications
              WHERE destination_uuid = (SELECT destination_uuid FROM oldest_entry)
                AND topic = (SELECT topic FROM oldest_entry)
                AND created_at_epoch_ms = (SELECT created_at_epoch_ms FROM oldest_entry)
              RETURNING message
            """
                .formatted(this.schema);

        Object[] recvdSermessage = null;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          stmt.setString(1, workflowUuid);
          stmt.setString(2, finalTopic);

          try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
              String serializedMessage = rs.getString("message");
              recvdSermessage = JSONUtil.deserializeToArray(serializedMessage);
            }
          }
        }

        // Record operation result
        Object toSave = recvdSermessage == null ? null : recvdSermessage[0];
        StepResult output =
            new StepResult(workflowUuid, functionId, functionName)
                .withOutput(JSONUtil.serialize(toSave));
        StepsDAO.recordStepResultTxn(
            output, startTime, System.currentTimeMillis(), conn, this.schema);

        conn.commit();
        return toSave;

      } catch (Exception e) {
        conn.rollback();
        throw e;
      }
    }
  }

  private void setEvent(
      Connection conn, String workflowId, int functionId, String key, String message)
      throws SQLException {
    final String eventSql =
        """
          INSERT INTO %s.workflow_events (workflow_uuid, key, value)
          VALUES (?, ?, ?)
          ON CONFLICT (workflow_uuid, key)
          DO UPDATE SET value = EXCLUDED.value
        """
            .formatted(this.schema);

    try (PreparedStatement stmt = conn.prepareStatement(eventSql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, key);
      stmt.setString(3, message);
      stmt.executeUpdate();
    }

    final String eventHistorySql =
        """
          INSERT INTO %s.workflow_events_history (workflow_uuid, function_id, key, value)
          VALUES (?, ?, ?, ?)
          ON CONFLICT (workflow_uuid, key, function_id)
          DO UPDATE SET value = EXCLUDED.value
        """
            .formatted(this.schema);

    try (PreparedStatement stmt = conn.prepareStatement(eventHistorySql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, functionId);
      stmt.setString(3, key);
      stmt.setString(4, message);
      stmt.executeUpdate();
    }
  }

  void setEvent(String workflowId, int functionId, String key, Object message, boolean asStep)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.setEvent";
    String serializedMessage = JSONUtil.serialize(message);

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

        this.setEvent(conn, workflowId, functionId, key, serializedMessage);

        if (asStep) {
          // Record the operation result
          StepResult output = new StepResult(workflowId, functionId, functionName);
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
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.getEvent";

    // Check for previous executions only if it's in a workflow
    if (callerCtx != null) {

      StepResult recordedOutput = null;

      try (Connection conn = dataSource.getConnection()) {
        recordedOutput =
            StepsDAO.checkStepExecutionTxn(
                callerCtx.getWorkflowId(),
                callerCtx.getFunctionId(),
                functionName,
                conn,
                this.schema);
      }

      if (recordedOutput != null) {
        logger.debug("Replaying getEvent, id: {}, key: {}", callerCtx.getFunctionId(), key);
        if (recordedOutput.output() != null) {
          Object[] outputArray = JSONUtil.deserializeToArray(recordedOutput.output());
          return outputArray == null ? null : outputArray[0];
        } else {
          throw new RuntimeException("No output recorded in the last getEvent");
        }
      } else {
        logger.debug("Running getEvent, id: {}, key: {}", callerCtx.getFunctionId(), key);
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
            SELECT value FROM %s.workflow_events WHERE workflow_uuid = ? AND key = ?
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
              Object[] valueArray = JSONUtil.deserializeToArray(serializedValue);
              value = valueArray == null ? null : valueArray[0];
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
              StepsDAO.sleep(
                      dataSource,
                      callerCtx.getWorkflowId(),
                      callerCtx.getTimeoutFunctionId(),
                      timeout,
                      true, // skip_sleep
                      this.schema)
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
        StepResult output =
            new StepResult(callerCtx.getWorkflowId(), callerCtx.getFunctionId(), functionName)
                .withOutput(JSONUtil.serialize(value));
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
