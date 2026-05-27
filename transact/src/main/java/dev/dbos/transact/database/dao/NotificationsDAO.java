package dev.dbos.transact.database.dao;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.GetEventCaller;
import dev.dbos.transact.database.SystemDatabase.NotificationRegistry;
import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.database.signal.SignalMap;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.NotificationInfo;
import dev.dbos.transact.workflow.SendMessage;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.joining;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationsDAO {

  private NotificationsDAO() {}

  private static final Logger logger = LoggerFactory.getLogger(NotificationsDAO.class);

  private static Map<String, Set<String>> findForkDescendantsTxn(
      Connection conn, String schema, List<String> workflowIds) throws SQLException {
    Map<String, List<String>> children = new HashMap<>();
    Set<String> seen = new LinkedHashSet<>(workflowIds);
    List<String> frontier = new ArrayList<>(new LinkedHashSet<>(workflowIds));

    while (!frontier.isEmpty()) {
      String placeholders = frontier.stream().map(x -> "?").collect(joining(","));
      String sql =
          """
          SELECT workflow_uuid, forked_from FROM "%s".workflow_status
          WHERE forked_from IN (%s)
          """.formatted(schema, placeholders);
      try (var stmt = conn.prepareStatement(sql)) {
        for (int i = 0; i < frontier.size(); i++) stmt.setString(i + 1, frontier.get(i));
        List<String> next = new ArrayList<>();
        try (var rs = stmt.executeQuery()) {
          while (rs.next()) {
            String forkedId = rs.getString("workflow_uuid");
            String forkedFrom = rs.getString("forked_from");
            children.computeIfAbsent(forkedFrom, k -> new ArrayList<>()).add(forkedId);
            if (seen.add(forkedId)) next.add(forkedId);
          }
        }
        frontier = next;
      }
    }

    Map<String, Set<String>> result = new LinkedHashMap<>();
    for (String root : workflowIds) {
      if (result.containsKey(root)) continue;
      Set<String> descendants = new LinkedHashSet<>();
      Deque<String> stack = new ArrayDeque<>(children.getOrDefault(root, List.of()));
      while (!stack.isEmpty()) {
        String node = stack.pop();
        if (!node.equals(root) && descendants.add(node)) {
          stack.addAll(children.getOrDefault(node, List.of()));
        }
      }
      result.put(root, descendants);
    }
    return result;
  }

  public static void sendBulk(
      DbContext ctx,
      List<SendMessage> messages,
      String workflowId,
      int stepId,
      String functionName,
      boolean sendToForks,
      String serialization)
      throws SQLException {

    if (messages.isEmpty()) {
      return;
    }

    // Reject duplicate idempotency keys within the batch
    var keys =
        messages.stream()
            .map(SendMessage::idempotencyKey)
            .filter(Objects::nonNull)
            .toList();
    if (keys.size() != keys.stream().distinct().count()) {
      throw new IllegalArgumentException("Duplicate idempotency keys within sendBulk batch");
    }

    DBOSSerializer serializer = ctx.serializer();
    var startTime = System.currentTimeMillis();

    // Serialize each message once
    record SerializedPair(SendMessage msg, SerializationUtil.SerializedResult serialized) {}
    List<SerializedPair> pairs = new ArrayList<>(messages.size());
    for (var msg : messages) {
      pairs.add(new SerializedPair(msg, SerializationUtil.serializeValue(msg.message(), serialization, serializer)));
    }

    try (Connection conn = ctx.getConnection()) {
      conn.setAutoCommit(false);
      try {
        // Check for replay if inside a workflow
        if (workflowId != null) {
          StepResult recorded =
              StepsDAO.checkStepResult(conn, ctx.schema(), workflowId, stepId, functionName);
          if (recorded != null) {
            logger.debug("Replaying sendBulk, workflowId: {}, stepId: {}", workflowId, stepId);
            conn.commit();
            return;
          }
        }

        // Collect all destination IDs for fork resolution
        Map<String, Set<String>> forkDescendants = Map.of();
        if (sendToForks) {
          List<String> destIds = pairs.stream().map(p -> p.msg().destinationId()).distinct().toList();
          forkDescendants = findForkDescendantsTxn(conn, ctx.schema(), destIds);
        }

        // Build insert rows: base dest + sorted descendants
        record InsertRow(String destId, SerializationUtil.SerializedResult serialized, String topic, String messageUuid) {}
        List<InsertRow> rows = new ArrayList<>();
        for (var pair : pairs) {
          var msg = pair.msg();
          String baseDest = msg.destinationId();
          String finalTopic = (msg.topic() != null) ? msg.topic() : Constants.DBOS_NULL_TOPIC;

          List<String> destinations = new ArrayList<>();
          destinations.add(baseDest);
          if (sendToForks) {
            var desc = forkDescendants.getOrDefault(baseDest, Set.of());
            desc.stream().sorted().forEach(destinations::add);
          }

          for (String dest : destinations) {
            String uuid;
            if (msg.idempotencyKey() != null) {
              uuid = sendToForks ? msg.idempotencyKey() + "::" + dest : msg.idempotencyKey();
            } else {
              uuid = UUID.randomUUID().toString();
            }
            rows.add(new InsertRow(dest, pair.serialized(), finalTopic, uuid));
          }
        }

        // Batch-insert all rows
        final String sql =
            """
              INSERT INTO "%s".notifications
                (destination_uuid, topic, message, serialization, message_uuid)
              VALUES (?, ?, ?, ?, ?)
              ON CONFLICT (message_uuid) DO NOTHING
            """.formatted(ctx.schema());

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          for (var row : rows) {
            stmt.setString(1, row.destId());
            stmt.setString(2, row.topic());
            stmt.setString(3, row.serialized().serializedValue());
            stmt.setString(4, row.serialized().serialization());
            stmt.setString(5, row.messageUuid());
            stmt.addBatch();
          }
          stmt.executeBatch();
        } catch (SQLException e) {
          if ("23503".equals(e.getSQLState())) {
            // Find which destination was missing
            String missingDest = rows.stream().map(InsertRow::destId).findFirst().orElse("unknown");
            throw new DBOSNonExistentWorkflowException(missingDest);
          }
          throw e;
        }

        if (workflowId != null) {
          var output = new StepResult(workflowId, stepId, functionName, null, null, null, null);
          StepsDAO.recordStepResult(conn, ctx.schema(), output, startTime, System.currentTimeMillis());
        }

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

  public static Object recv(
      DbContext ctx,
      String workflowId,
      int stepId,
      Duration timeout,
      int timeoutStepId,
      String topic,
      Duration dbPollingInterval,
      NotificationRegistry notifcationRegistry)
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

  private record GetEventResult(String value, String serialization) {}

  private static Optional<GetEventResult> getEvent(
      DbContext ctx, @NonNull String workflowId, @NonNull String key) throws SQLException {
    var sql =
        """
        SELECT value, serialization FROM "%s".workflow_events WHERE workflow_uuid = ? AND key = ?
        """
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, key);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          var value = rs.getString("value");
          var serialization = rs.getString("serialization");
          return Optional.of(new GetEventResult(value, serialization));
        }
      }
    }

    return Optional.empty();
  }

  public static Object getEvent(
      DbContext ctx,
      String workflowId,
      String key,
      Duration timeout,
      @Nullable GetEventCaller caller,
      Duration dbPollingInterval,
      NotificationRegistry notifcationRegistry)
      throws SQLException {

    if (Objects.requireNonNull(workflowId).isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    var stepName = "DBOS.getEvent";

    if (caller != null) {
      var prevResult =
          StepsDAO.checkStepResult(ctx, caller.workflowId(), caller.stepId(), stepName);
      if (prevResult != null) {
        logger.debug("Replaying getEvent, id: {}, key: {}", caller.stepId(), key);
        return prevResult.toResult(ctx.serializer());
      }
      logger.debug("Running getEvent, id: {}, key: {}", caller.stepId(), key);
    }

    var startTime = Instant.now();
    var eventKey = new SignalKey.Event(workflowId, key);
    dbPollingInterval = Objects.requireNonNullElse(dbPollingInterval, Duration.ofSeconds(1));

    GetEventResult result = null;
    try (var eventSignal = notifcationRegistry.subscribe(eventKey)) {
      while (true) {
        var optResult = getEvent(ctx, workflowId, key);
        if (optResult.isPresent()) {
          result = optResult.get();
          break;
        }

        // check cancelled (both workflowId and caller.workflowId)

        var sleepDuration =
            caller != null
                ? StepsDAO.durableSleepDuration(
                    ctx, caller.workflowId(), caller.timeoutStepId(), timeout)
                : timeout.minus(Duration.between(startTime, Instant.now()));

        if (sleepDuration.isNegative() || sleepDuration.isZero()) {
          var serialized = SerializationUtil.serializeValue(null, null, ctx.serializer());
          result = new GetEventResult(serialized.serializedValue(), serialized.serialization());
          break;
        }

        var loopDuration =
            dbPollingInterval.compareTo(sleepDuration) <= 0 ? dbPollingInterval : sleepDuration;

        SignalMap.awaitAny(loopDuration, eventSignal);
      }
    }

    Objects.requireNonNull(result);
    ctx.checkClosed();

    if (caller != null) {
      var stepResult =
          new StepResult(
              caller.workflowId(),
              caller.stepId(),
              stepName,
              result.value(),
              null,
              null,
              result.serialization());
      StepsDAO.recordStepResult(ctx, stepResult, startTime.toEpochMilli());
    }

    return SerializationUtil.deserializeValue(
        result.value(), result.serialization(), ctx.serializer());
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
