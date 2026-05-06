package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PostgresStepFactory {

  private static final Logger logger = LoggerFactory.getLogger(PostgresStepFactory.class);

  protected static void ensurePostgres(Connection conn) throws SQLException {
    var productName = conn.getMetaData().getDatabaseProductName();
    if (!productName.equalsIgnoreCase("PostgreSQL")) {
      throw new IllegalArgumentException(
          "TxStepFactory requires a PostgreSQL datasource, got: " + productName);
    }
  }

  protected static void ensureSchema(Connection conn, String schema) throws SQLException {
    Objects.requireNonNull(schema, "schema must not be null");
    var sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(schema, "schema must not be null"));
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return;
        }
      }
    }

    try (var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA \"%s\"".formatted(schema));
    }
  }

  protected static void ensureTxOutputTable(Connection conn, String schema) throws SQLException {
    Objects.requireNonNull(schema, "schema must not be null");
    var sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(schema, "schema must not be null"));
      stmt.setString(2, "tx_step_outputs");
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return;
        }
      }
    }

    logger.debug("Creating tx_step_outputs table in schema={}", schema);
    try (var stmt = conn.createStatement()) {
      stmt.execute(
          """
          CREATE TABLE "%1$s".tx_step_outputs (
            workflow_id TEXT NOT NULL,
            step_id INT NOT NULL,
            output TEXT,
            error TEXT,
            serialization TEXT,
            created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
            PRIMARY KEY (workflow_id, step_id)
          )"""
              .formatted(schema));
    }
  }

  protected final DBOS dbos;
  protected final String schema;
  protected final DBOSSerializer serializer;

  @FunctionalInterface
  protected interface ConnectionOpener {
    Connection open() throws SQLException;
  }

  protected PostgresStepFactory(
      DBOS dbos, String schema, DBOSSerializer serializer, ConnectionOpener opener) {
    this.dbos = Objects.requireNonNull(dbos);
    var config = dbos.integration().config();
    this.schema = SystemDatabase.sanitizeSchema(schema == null ? config.databaseSchema() : schema);
    this.serializer = serializer == null ? config.serializer() : serializer;
    try (var conn = opener.open()) {
      ensurePostgres(conn);
      ensureSchema(conn, this.schema);
      ensureTxOutputTable(conn, this.schema);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected String upsertSql() {
    return """
        INSERT INTO "%s".tx_step_outputs
          (workflow_id, step_id, output, error, serialization)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """
        .formatted(schema);
  }

  protected String checkSql() {
    return """
        SELECT output, error, serialization
        FROM "%s".tx_step_outputs
        WHERE workflow_id = ? AND step_id = ?
        """
        .formatted(schema);
  }

  protected abstract Optional<StepResult> checkExecution(
      String workflowId, int stepId, String stepName);

  protected abstract void recordError(String workflowId, int stepId, Exception exception);

  @FunctionalInterface
  protected interface TxStepFunction<R, X extends Exception> {
    R execute(String workflowId, int stepId) throws X;
  }

  @SuppressWarnings("unchecked")
  protected <R, X extends Exception> R runTxStep(TxStepFunction<R, X> execute, String stepName)
      throws X {
    return dbos.<R, X>runStep(
        () -> {
          var workflowId = Objects.requireNonNull(DBOS.workflowId());
          int stepId = Objects.requireNonNull(DBOS.stepId());

          var prev = checkExecution(workflowId, stepId, stepName);
          if (prev.isPresent()) {
            return prev.get().<R, X>toResult(serializer);
          }

          try {
            return execute.execute(workflowId, stepId);
          } catch (Exception e) {
            recordError(workflowId, stepId, e);
            throw (X) e;
          }
        },
        stepName);
  }
}
