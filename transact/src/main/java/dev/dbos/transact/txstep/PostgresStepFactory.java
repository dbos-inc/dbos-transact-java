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

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PostgresStepFactory<C> {

  private static final Logger DDL_LOGGER = LoggerFactory.getLogger(PostgresStepFactory.class);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  protected final DBOS dbos;
  protected final String schema;
  protected final DBOSSerializer serializer;

  protected static final String CHECK_SQL_TEMPLATE =
      """
      SELECT output, error, serialization
      FROM "%s".tx_step_outputs
      WHERE workflow_id = ? AND step_id = ?
      """;

  protected static final String UPSERT_SQL_TEMPLATE =
      """
      INSERT INTO "%s".tx_step_outputs
        (workflow_id, step_id, output, error, serialization)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT DO NOTHING
      """;

  public static void ensurePostgres(Connection conn) throws SQLException {
    var productName = conn.getMetaData().getDatabaseProductName();
    if (!productName.equalsIgnoreCase("PostgreSQL")) {
      throw new IllegalArgumentException(
          "PostgresStepFactory requires a PostgreSQL datasource, got: " + productName);
    }
  }

  public static boolean schemaExists(Connection conn, String schema) throws SQLException {
    var sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(schema, "schema must not be null"));
      try (var rs = stmt.executeQuery()) {
        return rs.next();
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
    DDL_LOGGER.debug("Creating tx_step_outputs table in schema={}", schema);
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

  protected PostgresStepFactory(
      DBOS dbos, @Nullable String rawSchema, @Nullable DBOSSerializer rawSerializer) {
    this.dbos = Objects.requireNonNull(dbos);
    var config = dbos.integration().config();
    this.schema =
        SystemDatabase.sanitizeSchema(rawSchema == null ? config.databaseSchema() : rawSchema);
    this.serializer = rawSerializer == null ? config.serializer() : rawSerializer;
  }

  protected abstract C openTransaction();

  protected abstract C openConnection();

  protected abstract void commit(C conn);

  protected abstract void rollback(C conn);

  protected abstract void close(C conn);

  protected abstract @Nullable StepResult checkExecution(
      String workflowId, int stepId, String stepName);

  protected abstract void recordResult(
      C conn, String workflowId, int stepId, String output, String error, String serialization);

  protected final <R> void recordOutput(C conn, String workflowId, int stepId, R retVal) {
    var value = SerializationUtil.serializeValue(retVal, null, serializer);
    recordResult(conn, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  protected final <E extends Exception> void recordError(
      String workflowId, int stepId, E exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    var conn = openConnection();
    try {
      recordResult(conn, workflowId, stepId, null, value.serializedValue(), value.serialization());
    } finally {
      close(conn);
    }
  }

  protected final <R, E extends Exception> R txStepInternal(
      ThrowingFunction<R, C, E> func, String stepName) throws E {
    var workflowId = Objects.requireNonNull(DBOS.workflowId());
    int stepId = Objects.requireNonNull(DBOS.stepId());

    logger.debug(
        "txStep starting: workflowId={} stepId={} stepName={}", workflowId, stepId, stepName);
    var prevResult = this.checkExecution(workflowId, stepId, stepName);
    if (prevResult != null) {
      logger.debug(
          "txStep cache hit: workflowId={} stepId={} stepName={}", workflowId, stepId, stepName);
      return prevResult.<R, E>toResult(serializer);
    }

    logger.debug(
        "txStep executing: workflowId={} stepId={} stepName={}", workflowId, stepId, stepName);
    var conn = openTransaction();
    try {
      var retVal = func.execute(conn);
      recordOutput(conn, workflowId, stepId, retVal);
      commit(conn);
      logger.debug(
          "txStep succeeded: workflowId={} stepId={} stepName={}", workflowId, stepId, stepName);
      return retVal;
    } catch (Exception e) {
      rollback(conn);
      recordError(workflowId, stepId, e);
      logger.debug(
          "txStep failed: workflowId={} stepId={} stepName={} error={}",
          workflowId,
          stepId,
          stepName,
          e.getMessage());
      throw e;
    } finally {
      close(conn);
    }
  }

  public <R, E extends Exception> R txStep(ThrowingFunction<R, C, E> func, String stepName)
      throws E {
    return dbos.<R, E>runStep(() -> txStepInternal(func, stepName), stepName);
  }

  public <E extends Exception> void txStep(ThrowingConsumer<C, E> func, String stepName) throws E {
    txStep(
        c -> {
          func.execute(c);
          return null;
        },
        stepName);
  }
}
