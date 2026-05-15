package dev.dbos.transact.database.dao;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.GetWorkflowEventContext;
import dev.dbos.transact.database.SystemDatabase.NotifcationRegistry;
import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.database.signal.SignalMap;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.NotificationInfo;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationsDAO {

  private NotificationsDAO() {}

  private static final Logger logger = LoggerFactory.getLogger(NotificationsDAO.class);

  public static void send(
      DbContext ctx,
      String workflowId,
      int stepId,
      String destinationId,
      Object message,
      String topic,
      String messageId,
      String serialization)
      throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.send";
    String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC;

    try (Connection conn = ctx.getConnection()) {
      conn.setAutoCommit(false);

      try {
        StepResult recordedOutput =
            StepsDAO.checkStepResult(conn, ctx.schema(), workflowId, stepId, functionName);

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
        var serializedMsg = SerializationUtil.serializeValue(message, serialization, serializer);

        final String sql =
            """
              INSERT INTO "%s".notifications
                (destination_uuid, topic, message, serialization, message_uuid)
              VALUES (?, ?, ?, ?, ?)
              ON CONFLICT (message_uuid) DO NOTHING
            """
                .formatted(ctx.schema());

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          stmt.setString(1, destinationId);
          stmt.setString(2, finalTopic);
          stmt.setString(3, serializedMsg.serializedValue());
          stmt.setString(4, serializedMsg.serialization());
          stmt.setString(5, finalMessageId);
          stmt.executeUpdate();
        } catch (SQLException e) {
          if ("23503".equals(e.getSQLState())) {
            throw new DBOSNonExistentWorkflowException(destinationId);
          }
          throw e;
        }

        var output = new StepResult(workflowId, stepId, functionName, null, null, null, null);
        StepsDAO.recordStepResult(
            conn, ctx.schema(), output, startTime, System.currentTimeMillis());

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

  public static void sendDirect(
      DbContext ctx,
      String destinationId,
      Object message,
      String topic,
      String messageId,
      String serialization)
      throws SQLException {
    DBOSSerializer serializer = ctx.serializer();
    String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC;
    String finalMessageId = (messageId != null) ? messageId : UUID.randomUUID().toString();
    var serializedMsg = SerializationUtil.serializeValue(message, serialization, serializer);

    final String sql =
        """
          INSERT INTO "%s".notifications
            (destination_uuid, topic, message, message_uuid, serialization)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT (message_uuid) DO NOTHING
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, destinationId);
      stmt.setString(2, finalTopic);
      stmt.setString(3, serializedMsg.serializedValue());
      stmt.setString(4, finalMessageId);
      stmt.setString(5, serializedMsg.serialization());
      stmt.executeUpdate();
    } catch (SQLException e) {
      if ("23503".equals(e.getSQLState())) {
        throw new DBOSNonExistentWorkflowException(destinationId);
      }
      throw e;
    }
  }

  public static Object recv(
      DbContext ctx,
      String workflowId,
      int stepId,
      Duration timeout,
      int timeoutStepId,
      String topic,
      Duration dbPollingInterval,
      NotifcationRegistry notifcationRegistry)
      throws SQLException {

    if (Objects.requireNonNull(workflowId).isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    var stepName = "DBOS.recv";
    topic = Objects.requireNonNullElse(topic, Constants.DBOS_NULL_TOPIC);

    var recordedResult = StepsDAO.checkStepResult(ctx, workflowId, stepId, stepName);
    if (recordedResult != null) {
      logger.debug(
          "Replaying recv, workflowId: {}, stepId: {}, topic: {}", workflowId, stepId, topic);
      if (recordedResult.output() != null) {
        return recordedResult.toResult(ctx.serializer());
      }
      logger.debug(
          "Running recv, workflowId: {}, stepId: {}, topic: {}", workflowId, stepId, topic);
    }

    var startTime = System.currentTimeMillis();
    var messageKey = new SignalKey.Message(workflowId, topic);
    dbPollingInterval = Objects.requireNonNullElse(dbPollingInterval, Duration.ofSeconds(1));

    try (var messageSignal = notifcationRegistry.subscribe(messageKey)) {
      while (true) {
        ctx.checkClosed();
        var sql =
            """
              SELECT topic FROM "%s".notifications
              WHERE destination_uuid = ? AND topic = ? AND consumed = FALSE
            """
                .formatted(ctx.schema());
        try (var conn = ctx.getConnection();
            var stmt = conn.prepareStatement(sql)) {
          stmt.setString(1, workflowId);
          stmt.setString(2, topic);
          try (var rs = stmt.executeQuery()) {
            if (rs.next()) {
              // query for results
              break;
            }
          }
        }

        // check cancelled

        var sleepDuration = StepsDAO.durableSleepDuration(ctx, workflowId, timeoutStepId, timeout);
        if (sleepDuration.isNegative() || sleepDuration.isZero()) {
          var output = SerializationUtil.serializeValue(null, null, ctx.serializer());
          var stepResult = StepResult.ofOutput(workflowId, stepId, stepName, output);
          StepsDAO.recordStepResult(ctx, stepResult, startTime);
          return null;
        }

        var loopDuration =
            dbPollingInterval.compareTo(sleepDuration) <= 0 ? dbPollingInterval : sleepDuration;

        SignalMap.awaitAny(loopDuration, messageSignal);
      }
    }

    ctx.checkClosed();
    var sql =
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
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection()) {
      conn.setAutoCommit(false);
      try {
        String serializedMessage = null;
        String serialization = null;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          stmt.setString(1, workflowId);
          stmt.setString(2, topic);
          stmt.setString(3, workflowId);
          stmt.setString(4, topic);

          // Note, if there are two executors running the same workflow waiting on the same recv,
          // only the first one will return a row here. The second one get a null message but then
          // throw a WorkflowExecutionConflictException when it records the step result.
          try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
              serializedMessage = rs.getString("message");
              serialization = rs.getString("serialization");
            }
          }
        }

        var deserializedMessage =
            SerializationUtil.deserializeValue(serializedMessage, serialization, ctx.serializer());

        var output =
            new StepResult(
                workflowId, stepId, stepName, serializedMessage, null, null, serialization);
        StepsDAO.recordStepResult(conn, ctx.schema(), output, startTime);

        conn.commit();
        return deserializedMessage;
      } catch (Exception e) {
        conn.rollback();
        throw e;
      }
    }
  }

  private static void setEvent(
      Connection conn,
      String schema,
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
            .formatted(schema);

    try (var stmt = conn.prepareStatement(eventSql)) {
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
            .formatted(schema);

    try (var stmt = conn.prepareStatement(eventHistorySql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, functionId);
      stmt.setString(3, key);
      stmt.setString(4, message);
      stmt.setString(5, serialization);
      stmt.executeUpdate();
    }
  }

  public static void setEvent(
      DbContext ctx,
      String workflowId,
      int functionId,
      String key,
      Object message,
      boolean asStep,
      String serialization)
      throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.setEvent";

    SerializationUtil.SerializedResult serializedResult =
        SerializationUtil.serializeValue(message, serialization, serializer);

    try (var conn = ctx.getConnection()) {
      conn.setAutoCommit(false);
      try {
        if (asStep) {
          var recordedOutput =
              StepsDAO.checkStepResult(conn, ctx.schema(), workflowId, functionId, functionName);
          if (recordedOutput != null) {
            logger.debug(
                "Replaying setEvent, workflow: {}, step: {}, key: {}", workflowId, functionId, key);
            conn.commit();
            return;
          } else {
            logger.debug(
                "Running setEvent, workflow: {}, step: {}, key: {}", workflowId, functionId, key);
          }
        }

        setEvent(
            conn,
            ctx.schema(),
            workflowId,
            functionId,
            key,
            serializedResult.serializedValue(),
            serializedResult.serialization());

        if (asStep) {
          StepResult output =
              new StepResult(workflowId, functionId, functionName, null, null, null, null);
          StepsDAO.recordStepResult(conn, ctx.schema(), output, startTime);
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

  public static Object getEvent(
      DbContext ctx,
      NotifcationRegistry notifcationRegistry,
      Duration dbPollingInterval,
      String targetUuid,
      String key,
      Duration timeout,
      GetWorkflowEventContext callerCtx)
      throws SQLException {

    return null;
    // DBOSSerializer serializer = ctx.serializer();
    // var startTime = System.currentTimeMillis();
    // String functionName = "DBOS.getEvent";

    // if (callerCtx != null) {
    //   StepResult recordedOutput;
    //   try (Connection conn = ctx.getConnection()) {
    //     recordedOutput =
    //         StepsDAO.checkStepExecutionTxn(
    //             conn, ctx.schema(), callerCtx.workflowId(), callerCtx.functionId(),
    // functionName);
    //   }

    //   if (recordedOutput != null) {
    //     logger.debug("Replaying getEvent, id: {}, key: {}", callerCtx.functionId(), key);
    //     if (recordedOutput.output() != null) {
    //       return SerializationUtil.deserializeValue(
    //           recordedOutput.output(), recordedOutput.serialization(), serializer);
    //     } else {
    //       throw new RuntimeException("No output recorded in the last getEvent");
    //     }
    //   } else {
    //     logger.debug("Running getEvent, id: {}, key: {}", callerCtx.functionId(), key);
    //   }
    // }

    // String payload = targetUuid + "::" + key;
    // NotificationListenerService.LockConditionPair lockConditionPair =
    //     notificationService.getOrCreateNotificationCondition(payload);

    // lockConditionPair.lock.lock();
    // try {
    //   Object value = null;
    //   final String sql =
    //       """
    //         SELECT value, serialization FROM "%s".workflow_events WHERE workflow_uuid = ? AND key
    // = ?
    //       """
    //           .formatted(ctx.schema());

    //   double actualTimeout =
    //       Objects.requireNonNull(timeout, "getEvent timeout cannot be null").toMillis();
    //   var targetTime = System.currentTimeMillis() + actualTimeout;
    //   var checkedDBForSleep = false;
    //   var hasExistingNotification = false;

    //   while (true) {
    //     if (ctx.isClosed()) throw new IllegalStateException("SystemDatabase is closed");
    //     try (Connection conn = ctx.getConnection();
    //         PreparedStatement stmt = conn.prepareStatement(sql)) {

    //       stmt.setString(1, targetUuid);
    //       stmt.setString(2, key);

    //       try (ResultSet rs = stmt.executeQuery()) {
    //         if (rs.next()) {
    //           String serializedValue = rs.getString("value");
    //           String serialization = rs.getString("serialization");
    //           value =
    //               SerializationUtil.deserializeValue(serializedValue, serialization, serializer);
    //           hasExistingNotification = true;
    //         }
    //       }
    //     }

    //     if (hasExistingNotification) break;
    //     var nowTime = System.currentTimeMillis();
    //     if (nowTime > targetTime) break;

    //     if (callerCtx != null && !checkedDBForSleep) {
    //       actualTimeout =
    //           StepsDAO.durableSleepDuration(
    //                   ctx, callerCtx.workflowId(), callerCtx.timeoutFunctionId(), timeout)
    //               .toMillis();
    //       targetTime = System.currentTimeMillis() + actualTimeout;
    //       checkedDBForSleep = true;
    //       if (nowTime > targetTime) break;
    //     }

    //     try {
    //       long timeoutms = (long) (targetTime - nowTime);
    //       logger.debug("Waiting for notification {}...", timeout);
    //       lockConditionPair.condition.await(
    //           Math.min(timeoutms, dbPollingInterval.toMillis()), TimeUnit.MILLISECONDS);
    //     } catch (InterruptedException e) {
    //       Thread.currentThread().interrupt();
    //       throw new RuntimeException("Interrupted while waiting for event", e);
    //     }
    //   }

    //   if (callerCtx != null) {
    //     var toSaveSer = SerializationUtil.serializeValue(value, null, serializer);
    //     StepResult output =
    //         new StepResult(
    //                 callerCtx.workflowId(),
    //                 callerCtx.functionId(),
    //                 functionName,
    //                 null,
    //                 null,
    //                 null,
    //                 toSaveSer.serialization())
    //             .withOutput(toSaveSer.serializedValue());
    //     StepsDAO.recordStepResultTxn(ctx, output, startTime, System.currentTimeMillis());
    //   }

    //   return value;

    // } finally {
    //   lockConditionPair.lock.unlock();
    //   notificationService.unregisterNotificationCondition(payload);
    // }
  }

  public static List<NotificationInfo> getAllNotifications(DbContext ctx, String workflowId)
      throws SQLException {
    DBOSSerializer serializer = ctx.serializer();
    var sql =
        """
        SELECT topic, message, serialization, created_at_epoch_ms, consumed
        FROM "%s".notifications
        WHERE destination_uuid = ?
        ORDER BY created_at_epoch_ms
        """
            .formatted(ctx.schema());

    var notifications = new ArrayList<NotificationInfo>();
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          var rawTopic = rs.getString("topic");
          var topic = Constants.DBOS_NULL_TOPIC.equals(rawTopic) ? null : rawTopic;
          var serialization = rs.getString("serialization");
          var message =
              SerializationUtil.deserializeValue(
                  rs.getString("message"), serialization, serializer);
          var createdAtEpochMs = rs.getLong("created_at_epoch_ms");
          var consumed = rs.getBoolean("consumed");
          notifications.add(
              new NotificationInfo(
                  topic,
                  message,
                  createdAtEpochMs != 0 ? Instant.ofEpochMilli(createdAtEpochMs) : null,
                  consumed));
        }
      }
    }
    return notifications;
  }
}
