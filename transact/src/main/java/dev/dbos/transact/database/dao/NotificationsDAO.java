package dev.dbos.transact.database.dao;

import static java.util.stream.Collectors.joining;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.GetEventCaller;
import dev.dbos.transact.database.SendCaller;
import dev.dbos.transact.database.SqlTransaction;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.database.signal.SignalMap;
import dev.dbos.transact.database.signal.Subscription;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
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
import java.util.function.Function;

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
          """
              .formatted(schema, placeholders);
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

  /** Sends messages from {@code caller}, or from outside any workflow when it is null. */
  public static void sendBulk(
      DbContext ctx,
      List<SendMessage> messages,
      @Nullable SendCaller caller,
      String functionName,
      boolean sendToForks,
      String serialization)
      throws SQLException {

    if (messages.isEmpty()) {
      return;
    }
    // Reject duplicate idempotency keys within the batch
    var keys = messages.stream().map(SendMessage::idempotencyKey).filter(Objects::nonNull).toList();
    if (keys.size() != keys.stream().distinct().count()) {
      throw new IllegalArgumentException("Duplicate idempotency keys within sendBulk batch");
    }

    DBOSSerializer serializer = ctx.serializer();
    var startTime = System.currentTimeMillis();

    // Serialize each message once
    record SerializedPair(SendMessage msg, SerializationUtil.SerializedResult serialized) {}
    List<SerializedPair> pairs = new ArrayList<>(messages.size());
    for (var msg : messages) {
      pairs.add(
          new SerializedPair(
              msg, SerializationUtil.serializeValue(msg.message(), serialization, serializer)));
    }

    try (var txConn = ctx.getConnection()) {
      SqlTransaction.run(
          txConn,
          conn -> {
            // A send from the workflow body replays as the step it was recorded as.
            if (caller instanceof SendCaller.FromWorkflow step) {
              StepResult recorded =
                  StepsDAO.checkStepResult(
                      conn,
                      ctx.schema(),
                      step.workflowId(),
                      step.stepId(),
                      functionName,
                      step.ownerXid());
              if (recorded != null) {
                logger.debug(
                    "Replaying sendBulk, workflowId: {}, stepId: {}",
                    step.workflowId(),
                    step.stepId());
                return;
              }
            }

            // Collect all destination IDs for fork resolution
            Map<String, Set<String>> forkDescendants = Map.of();
            if (sendToForks) {
              List<String> destIds =
                  pairs.stream().map(p -> p.msg().destinationId()).distinct().toList();
              forkDescendants = findForkDescendantsTxn(conn, ctx.schema(), destIds);
            }

            // Build insert rows: base dest + sorted descendants
            record InsertRow(
                String destId,
                SerializationUtil.SerializedResult serialized,
                String topic,
                String messageUuid) {}
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
                var wfid =
                    msg.idempotencyKey() != null
                        ? msg.idempotencyKey() + "::" + dest
                        : UUID.randomUUID().toString();
                rows.add(new InsertRow(dest, pair.serialized(), finalTopic, wfid));
              }
            }

            // Batch-insert all rows
            final String sql =
                """
              INSERT INTO "%s".notifications
                (destination_uuid, topic, message, serialization, message_uuid)
              VALUES (?, ?, ?, ?, ?)
              ON CONFLICT (message_uuid) DO NOTHING
            """
                    .formatted(ctx.schema());

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
                var distinctDests = rows.stream().map(InsertRow::destId).distinct().toList();
                throw new DBOSNonExistentWorkflowException(
                    distinctDests.size() == 1 ? distinctDests.get(0) : null);
              }
              throw e;
            }

            // The owner is checked after the messages, so the status row is locked last, as every
            // fenced write locks it. Recording the step checks it first; a send from inside a step
            // has no step to record, so it is checked on its own.
            if (caller instanceof SendCaller.FromWorkflow step) {
              var output =
                  new StepResult(
                      step.workflowId(), step.stepId(), functionName, null, null, null, null);
              StepsDAO.recordStepResult(
                  ctx, conn, output, startTime, System.currentTimeMillis(), step.ownerXid());
            } else if (caller != null) {
              WorkflowDAO.checkOwner(conn, ctx.schema(), caller.workflowId(), caller.ownerXid());
            }
          });
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
      Function<SignalKey, Subscription> createSubscription,
      String ownerXid)
      throws SQLException {

    if (Objects.requireNonNull(workflowId).isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }
    Objects.requireNonNull(dbPollingInterval);

    var stepName = "DBOS.recv";
    final var recvTopic = Objects.requireNonNullElse(topic, Constants.DBOS_NULL_TOPIC);

    var result = StepsDAO.checkStepResult(ctx, workflowId, stepId, stepName, ownerXid);
    if (result != null) {
      logger.debug(
          "Replaying recv, workflowId: {}, stepId: {}, topic: {}", workflowId, stepId, recvTopic);
      // A NULL output is a recv that claimed no message (a peer took it) and returned null.
      // Replay that rather than receiving again: a second run would record a different result
      // for this step.
      return result.output() != null ? result.toResult(ctx.serializer()) : null;
    }

    logger.debug(
        "Running recv, workflowId: {}, stepId: {}, topic: {}", workflowId, stepId, recvTopic);

    var startTime = System.currentTimeMillis();
    var messageKey = new SignalKey.Message(workflowId, recvTopic);
    var selectSql =
        """
          SELECT topic FROM "%s".notifications
          WHERE destination_uuid = ? AND topic = ? AND consumed = FALSE
        """
            .formatted(ctx.schema());

    var timeoutAt = StepsDAO.durableDeadline(ctx, workflowId, timeoutStepId, timeout, ownerXid);
    while (true) {
      ctx.checkClosed();
      try (var messageSignal = createSubscription.apply(messageKey)) {
        try (var permit = ctx.acquirePollPermit();
            var conn = ctx.getConnection();
            var stmt = conn.prepareStatement(selectSql)) {
          stmt.setString(1, workflowId);
          stmt.setString(2, recvTopic);
          try (var rs = stmt.executeQuery()) {
            if (rs.next()) {
              // query for results
              break;
            }
          }
          WorkflowDAO.checkWorkflow(conn, ctx.schema(), workflowId, ownerXid);
        }

        var duration = Duration.between(Instant.now(), timeoutAt);
        if (duration.isNegative() || duration.isZero()) {
          var output = SerializationUtil.serializeValue(null, null, ctx.serializer());
          var stepResult = StepResult.ofOutput(workflowId, stepId, stepName, output);
          StepsDAO.recordStepResult(
              ctx, stepResult, startTime, System.currentTimeMillis(), ownerXid);
          return null;
        }

        var loopDuration =
            dbPollingInterval.compareTo(duration) <= 0 ? dbPollingInterval : duration;

        SignalMap.awaitAny(loopDuration, messageSignal);
      }
    }

    ctx.checkClosed();
    WorkflowDAO.checkWorkflow(ctx, workflowId, ownerXid);

    var updateSql =
        """
          UPDATE "%1$s".notifications
          SET consumed = TRUE, consumed_by_function_id = ?
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

    try (var txConn = ctx.getConnection()) {
      // Only the claim is replayed, never the wait above it: re-running the whole method would
      // register a fresh subscription and restart the caller's timeout from zero. A peer that won
      // the message between attempts lands this on the null-message branch below, which is a case
      // it already handles.
      return SystemDatabase.retryOnSerializationError(
          "recv claim",
          () ->
              SqlTransaction.call(
                  txConn,
                  conn -> {
                    String serializedMessage = null;
                    String serialization = null;
                    try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
                      // The consuming step is recorded so a rewind can delete exactly the
                      // messages consumed by the steps it discards.
                      stmt.setInt(1, stepId);
                      stmt.setString(2, workflowId);
                      stmt.setString(3, recvTopic);
                      stmt.setString(4, workflowId);
                      stmt.setString(5, recvTopic);

                      // Recording the step checks that this execution still owns the workflow, so
                      // a stale execution's claim rolls back and leaves the message for the
                      // owner.
                      try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                          serializedMessage = rs.getString("message");
                          serialization = rs.getString("serialization");
                        }
                      }
                    }

                    var deserializedMessage =
                        SerializationUtil.deserializeValue(
                            serializedMessage, serialization, ctx.serializer());

                    var output =
                        new StepResult(
                            workflowId,
                            stepId,
                            stepName,
                            serializedMessage,
                            null,
                            null,
                            serialization);
                    StepsDAO.recordStepResult(
                        ctx, conn, output, startTime, System.currentTimeMillis(), ownerXid);

                    return deserializedMessage;
                  }));
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
      String serialization,
      String ownerXid)
      throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.setEvent";

    SerializationUtil.SerializedResult serializedResult =
        SerializationUtil.serializeValue(message, serialization, serializer);

    try (var txConn = ctx.getConnection()) {
      try {
        SqlTransaction.run(
            txConn,
            conn -> {
              if (asStep) {
                var recordedOutput =
                    StepsDAO.checkStepResult(
                        conn, ctx.schema(), workflowId, functionId, functionName, ownerXid);
                if (recordedOutput != null) {
                  logger.debug(
                      "Replaying setEvent, workflow: {}, step: {}, key: {}",
                      workflowId,
                      functionId,
                      key);
                  return;
                } else {
                  logger.debug(
                      "Running setEvent, workflow: {}, step: {}, key: {}",
                      workflowId,
                      functionId,
                      key);
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

              // The owner is checked after the event, so the status row is locked last, as every
              // fenced write locks it. Recording the step checks it first; from inside a step there
              // is no step of its own to record, so it is checked on its own: an execution that has
              // lost its workflow sets nothing.
              if (asStep) {
                StepResult output =
                    new StepResult(workflowId, functionId, functionName, null, null, null, null);
                StepsDAO.recordStepResult(
                    ctx, conn, output, startTime, System.currentTimeMillis(), ownerXid);
              } else {
                WorkflowDAO.checkOwner(conn, ctx.schema(), workflowId, ownerXid);
              }
            });
      } catch (DBOSWorkflowExecutionConflictException e) {
        // Refused because this execution no longer owns the workflow: expected, not an error.
        logger.debug(
            "setEvent refused, workflow: {} id: {}, key: {}", workflowId, functionId, key, e);
        throw e;
      } catch (Exception e) {
        // The helper has already rolled back; this only records which call lost its work.
        logger.error(
            "setEvent rollback, workflow: {} id: {}, key: {}", workflowId, functionId, key, e);
        throw e;
      }
    }
  }

  private record GetEventResult(String value, String serialization) {}

  private static Optional<GetEventResult> getEvent(
      Connection conn, String schema, @NonNull String workflowId, @NonNull String key)
      throws SQLException {
    var sql =
        """
        SELECT value, serialization FROM "%s".workflow_events WHERE workflow_uuid = ? AND key = ?
        """
            .formatted(schema);
    try (var stmt = conn.prepareStatement(sql)) {
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
      Function<SignalKey, Subscription> notificationRegistry)
      throws SQLException {

    if (Objects.requireNonNull(workflowId).isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }
    Objects.requireNonNull(dbPollingInterval);

    var stepName = "DBOS.getEvent";

    if (caller != null) {
      var prevResult =
          StepsDAO.checkStepResult(
              ctx, caller.workflowId(), caller.stepId(), stepName, caller.ownerXid());
      if (prevResult != null) {
        logger.debug(
            "Replaying getEvent, workflowId: {}, stepId: {}, key: {}",
            caller.workflowId(),
            caller.stepId(),
            key);
        return prevResult.toResult(ctx.serializer());
      }
      logger.debug(
          "Running getEvent, workflowId: {}, stepId: {}, key: {}",
          caller.workflowId(),
          caller.stepId(),
          key);
    }

    var startTime = System.currentTimeMillis();
    var eventKey = new SignalKey.Event(workflowId, key);
    var timeoutAt =
        caller != null
            ? StepsDAO.durableDeadline(
                ctx, caller.workflowId(), caller.timeoutStepId(), timeout, caller.ownerXid())
            // Held at the largest instant, as inside a workflow, rather than overflowing.
            : Instant.ofEpochMilli(StepsDAO.deadlineEpochMs(System.currentTimeMillis(), timeout));
    GetEventResult result = null;

    while (true) {
      ctx.checkClosed();
      try (var eventSignal = notificationRegistry.apply(eventKey)) {
        try (var permit = ctx.acquirePollPermit();
            var conn = ctx.getConnection()) {

          var optResult = getEvent(conn, ctx.schema(), workflowId, key);
          if (optResult.isPresent()) {
            result = optResult.get();
            break;
          }

          if (caller != null) {
            WorkflowDAO.checkWorkflow(conn, ctx.schema(), caller.workflowId(), caller.ownerXid());
          }
        }

        var duration = Duration.between(Instant.now(), timeoutAt);
        if (duration.isNegative() || duration.isZero()) {
          var serialized = SerializationUtil.serializeValue(null, null, ctx.serializer());
          result = new GetEventResult(serialized.serializedValue(), serialized.serialization());
          break;
        }

        var loopDuration =
            dbPollingInterval.compareTo(duration) <= 0 ? dbPollingInterval : duration;

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
      StepsDAO.recordStepResult(
          ctx, stepResult, startTime, System.currentTimeMillis(), caller.ownerXid());
    }

    return SerializationUtil.deserializeValue(
        result.value(), result.serialization(), ctx.serializer());
  }
}
