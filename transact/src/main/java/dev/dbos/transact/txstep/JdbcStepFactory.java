package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.ThrowingConsumer;
import dev.dbos.transact.execution.ThrowingFunction;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcStepFactory {

  private static final Logger logger = LoggerFactory.getLogger(JdbcStepFactory.class);

  private final DBOS dbos;
  private final DataSource dataSource;
  private final String schema;
  private final DBOSSerializer serializer;

  public JdbcStepFactory(DBOS dbos, DataSource dataSource) {
    this(dbos, dataSource, null, null);
  }

  public JdbcStepFactory(DBOS dbos, DataSource dataSource, String schema) {
    this(dbos, dataSource, schema, null);
  }

  public JdbcStepFactory(DBOS dbos, DataSource dataSource, DBOSSerializer serializer) {
    this(dbos, dataSource, null, serializer);
  }

  public JdbcStepFactory(
      DBOS dbos, DataSource dataSource, String schema, DBOSSerializer serializer) {
    this.dbos = Objects.requireNonNull(dbos);
    this.dataSource = Objects.requireNonNull(dataSource);
    var config = dbos.integration().config();
    this.schema = SystemDatabase.sanitizeSchema(schema == null ? config.databaseSchema() : schema);
    this.serializer = serializer == null ? config.serializer() : serializer;

    createTxOutputTable(dataSource, this.schema);
  }

  public static void createTxOutputTable(DataSource dataSource, String schema) {
    try (var conn = dataSource.getConnection()) {
      ensureSchema(conn, schema);
      ensureTxOutputTable(conn, schema);
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  public static boolean schemaExists(Connection conn, String schema) throws SQLException {
    var sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(schema, "schema must not be null"));
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return true;
        } else {
          return false;
        }
      }
    }
  }

  public static void ensureSchema(Connection conn, String schema) throws SQLException {
    Objects.requireNonNull(schema, "schema must not be null");
    if (!schemaExists(conn, schema)) {
      try (var stmt = conn.createStatement()) {
        stmt.execute("CREATE SCHEMA IF NOT EXISTS \"%s\"".formatted(schema));
      }
    }
  }

  public static boolean tableExists(Connection conn, String schema) throws SQLException {
    var sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(schema, "schema must not be null"));
      stmt.setString(2, Objects.requireNonNull("tx_step_outputs", "tableName must not be null"));
      try (var rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  public static void ensureTxOutputTable(Connection conn, String schema) throws SQLException {
    Objects.requireNonNull(schema, "schema must not be null");
    if (tableExists(conn, schema)) {
      return;
    }
    try (var stmt = conn.createStatement()) {
      var ddlSql =
          """
          CREATE TABLE IF NOT EXISTS "%1$s".tx_step_outputs (
            workflow_id TEXT NOT NULL,
            step_id INT NOT NULL,
            output TEXT,
            error TEXT,
            serialization TEXT,
            created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
            PRIMARY KEY (workflow_id, step_id)
          )"""
              .formatted(schema);
      stmt.execute(ddlSql);
    }
  }

  private static class DBOSSqlException extends RuntimeException {
    public DBOSSqlException(SQLException wrappedException) {
      super(wrappedException.getMessage(), wrappedException);
    }
  }

  // helper methods that wrap SQLExceptions in WrappedSqlException to distinguish from app
  // exceptions
  private Connection getConnection() {
    try {
      var conn = dataSource.getConnection();
      conn.setAutoCommit(false);
      return conn;
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  private static void commit(Connection conn) {
    try {
      conn.commit();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  private static void rollback(Connection conn) {
    try {
      conn.rollback();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  private static void close(Connection conn) {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  private @Nullable StepResult checkExecution(String workflowId, int stepId, String stepName) {
    var sql =
        """
        SELECT output, error, serialization
        FROM "%s".tx_step_outputs
        WHERE workflow_id = ? AND step_id = ?
        """
            .formatted(this.schema);
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          var output = rs.getString("output");
          var error = rs.getString("error");
          var serialization = rs.getString("serialization");
          var result =
              new StepResult(workflowId, stepId, stepName, output, error, null, serialization);
          return result;
        } else {
          return null;
        }
      }
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  private <R> void recordResult(
      Connection conn,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    if (output != null && error != null) {
      throw new IllegalArgumentException("attempted to record non null output and error result");
    }

    String sql =
        """
        INSERT INTO "%s".tx_step_outputs
            (workflow_id, step_id, output, error, serialization)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """
            .formatted(schema);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      stmt.setString(3, output);
      stmt.setString(4, error);
      stmt.setString(5, serialization);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new DBOSSqlException(e);
    }
  }

  private <R> void recordOutput(Connection conn, String workflowId, int stepId, R retVal) {
    var value = SerializationUtil.serializeValue(retVal, null, serializer);
    recordResult(conn, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  private <E extends Exception> void recordError(String workflowId, int stepId, E exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    var conn = getConnection();
    try {
      recordResult(conn, workflowId, stepId, null, value.serializedValue(), value.serialization());
    } finally {
      close(conn);
    }
  }

  private <R, E extends Exception> R txStepInternal(
      ThrowingFunction<R, Connection, E> func, String stepName) throws E {
    var workflowId = Objects.requireNonNull(DBOS.workflowId());
    int stepId = Objects.requireNonNull(DBOS.stepId());

    var prevResult = this.checkExecution(workflowId, stepId, stepName);
    if (prevResult != null) {
      return prevResult.<R, E>toResult(serializer);
    }

    var conn = getConnection();
    try {
      var retVal = func.execute(conn);
      recordOutput(conn, workflowId, stepId, retVal);
      commit(conn);
      return retVal;
    } catch (Exception e) {
      rollback(conn);
      recordError(workflowId, stepId, e);
      throw e;
    } finally {
      close(conn);
    }
  }

  public <R, E extends Exception> R txStep(ThrowingFunction<R, Connection, E> func, String stepName)
      throws E {
    return dbos.<R, E>runStep(
        () -> {
          return this.txStepInternal(func, stepName);
        },
        stepName);
  }

  public <E extends Exception> void txStep(ThrowingConsumer<Connection, E> func, String stepName)
      throws E {
    txStep(
        c -> {
          func.execute(c);
          return null;
        },
        stepName);
  }
}
