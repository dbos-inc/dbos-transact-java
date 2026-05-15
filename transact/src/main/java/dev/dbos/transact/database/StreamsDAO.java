package dev.dbos.transact.database;

import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class StreamsDAO {

  private StreamsDAO() {}

  static void writeStreamFromStep(
      DbContext ctx,
      String workflowId,
      int functionId,
      String key,
      Object value,
      String serializationFormat)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      insertStream(conn, ctx.schema(), workflowId, functionId, key, value, serializationFormat);
    }
  }

  static void writeStreamFromWorkflow(
      DbContext ctx,
      String workflowId,
      int functionId,
      String key,
      Object value,
      String serializationFormat)
      throws SQLException {
    String functionName =
        STREAM_CLOSED_SENTINEL.equals(value) ? "DBOS.closeStream" : "DBOS.writeStream";
    long startTime = System.currentTimeMillis();

    try (var conn = ctx.getConnection()) {
      conn.setAutoCommit(false);

      try {
        StepResult recordedOutput =
            StepsDAO.checkStepResult(conn, ctx.schema(), workflowId, functionId, functionName);

        if (recordedOutput != null) {
          logger.debug("Replaying writeStream, id: {}, key: {}", functionId, key);
          conn.commit();
          return;
        } else {
          logger.debug("Running writeStream, id: {}, key: {}", functionId, key);
        }

        insertStream(conn, ctx.schema(), workflowId, functionId, key, value, serializationFormat);

        var output = new StepResult(workflowId, functionId, functionName, null, null, null, null);
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

  private static void insertStream(
      Connection conn,
      String schema,
      String workflowId,
      int functionId,
      String key,
      Object value,
      String serializationFormat)
      throws SQLException {
    var serialized = SerializationUtil.serializeValue(value, serializationFormat, null);
    int offset = getNextOffsetTx(conn, schema, workflowId, key);

    String sql =
        """
        INSERT INTO "%s".streams (workflow_uuid, key, value, "offset", function_id, serialization)
        VALUES (?, ?, ?, ?, ?, ?)
        """
            .formatted(schema);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, key);
      stmt.setString(3, serialized.serializedValue());
      stmt.setInt(4, offset);
      stmt.setInt(5, functionId);
      stmt.setString(6, serialized.serialization());
      stmt.executeUpdate();
    }
  }

  private static int getNextOffsetTx(Connection conn, String schema, String workflowId, String key)
      throws SQLException {
    String sql =
        """
        SELECT COALESCE(MAX("offset"), -1) + 1
        FROM "%s".streams
        WHERE workflow_uuid = ? AND key = ?
        """
            .formatted(schema);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, key);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1);
        }
        return 0;
      }
    }
  }

  static void closeStream(DbContext ctx, String workflowId, int functionId, String key)
      throws SQLException {
    writeStreamFromWorkflow(
        ctx, workflowId, functionId, key, STREAM_CLOSED_SENTINEL, "portable_json");
  }

  static Object readStream(DbContext ctx, String workflowId, String key, int offset)
      throws SQLException {
    String sql =
        """
        SELECT value, serialization
        FROM "%s".streams
        WHERE workflow_uuid = ? AND key = ? AND "offset" = ?
        """
            .formatted(ctx.schema());

    try (Connection conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, key);
      stmt.setInt(3, offset);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          String value = rs.getString("value");
          String serialization = rs.getString("serialization");
          Object deserialized = SerializationUtil.deserializeValue(value, serialization, null);
          if (STREAM_CLOSED_SENTINEL.equals(deserialized)) {
            throw new IllegalStateException("Stream closed for key: " + key);
          }
          return deserialized;
        }
        throw new IllegalArgumentException(
            "No value found for workflow=" + workflowId + ", key=" + key + ", offset=" + offset);
      }
    }
  }

  static Map<String, List<Object>> getAllStreamEntries(DbContext ctx, String workflowId)
      throws SQLException {
    String sql =
        """
        SELECT key, value, serialization
        FROM "%s".streams
        WHERE workflow_uuid = ?
        ORDER BY key, "offset"
        """
            .formatted(ctx.schema());

    var streams = new LinkedHashMap<String, List<Object>>();
    try (Connection conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          String key = rs.getString("key");
          String value = rs.getString("value");
          String serialization = rs.getString("serialization");

          Object deserialized = SerializationUtil.deserializeValue(value, serialization, null);
          if (STREAM_CLOSED_SENTINEL.equals(deserialized)) {
            continue;
          }

          streams.computeIfAbsent(key, k -> new ArrayList<>()).add(deserialized);
        }
      }
    }
    return streams;
  }

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(StreamsDAO.class);

  static final String STREAM_CLOSED_SENTINEL = "__DBOS_STREAM_CLOSED__";
}
