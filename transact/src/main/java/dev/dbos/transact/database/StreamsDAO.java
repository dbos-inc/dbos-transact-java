package dev.dbos.transact.database;

import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

class StreamsDAO {

  private final DataSource dataSource;
  private final String schema;

  StreamsDAO(DataSource dataSource, String schema) {
    this.dataSource = dataSource;
    this.schema = schema;
  }

  public void writeStreamFromStep(
      String workflowId, int functionId, String key, Object value, String serializationFormat)
      throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      insertStream(conn, workflowId, functionId, key, value, serializationFormat);
    }
  }

  public void writeStreamFromWorkflow(
      String workflowId, int functionId, String key, Object value, String serializationFormat)
      throws SQLException {
    String functionName =
        STREAM_CLOSED_SENTINEL.equals(value) ? "DBOS.closeStream" : "DBOS.writeStream";
    long startTime = System.currentTimeMillis();

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);

      try {
        StepResult recordedOutput =
            StepsDAO.checkStepExecutionTxn(workflowId, functionId, functionName, conn, schema);

        if (recordedOutput != null) {
          logger.debug("Replaying writeStream, id: {}, key: {}", functionId, key);
          conn.commit();
          return;
        } else {
          logger.debug("Running writeStream, id: {}, key: {}", functionId, key);
        }

        insertStream(conn, workflowId, functionId, key, value, serializationFormat);

        var output = new StepResult(workflowId, functionId, functionName, null, null, null, null);
        StepsDAO.recordStepResultTxn(output, startTime, System.currentTimeMillis(), conn, schema);

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

  private void insertStream(
      Connection conn,
      String workflowId,
      int functionId,
      String key,
      Object value,
      String serializationFormat)
      throws SQLException {
    var serialized = SerializationUtil.serializeValue(value, serializationFormat, null);
    int offset = getNextOffsetTx(conn, workflowId, key);

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

  private int getNextOffsetTx(Connection conn, String workflowId, String key) throws SQLException {
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

  public void closeStream(String workflowId, int functionId, String key) throws SQLException {
    writeStreamFromWorkflow(workflowId, functionId, key, STREAM_CLOSED_SENTINEL, "portable_json");
  }

  public Object readStream(String workflowId, String key, int offset) throws SQLException {
    String sql =
        """
        SELECT value, serialization
        FROM "%s".streams
        WHERE workflow_uuid = ? AND key = ? AND "offset" = ?
        """
            .formatted(schema);

    try (Connection conn = dataSource.getConnection();
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

  public Map<String, List<Object>> getAllStreamEntries(String workflowId) throws SQLException {
    String sql =
        """
        SELECT key, value, serialization
        FROM "%s".streams
        WHERE workflow_uuid = ?
        ORDER BY key, "offset"
        """
            .formatted(schema);

    var streams = new LinkedHashMap<String, List<Object>>();
    try (Connection conn = dataSource.getConnection();
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
