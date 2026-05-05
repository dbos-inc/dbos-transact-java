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

/**
 * Abstract base class for PostgreSQL-backed transactional step factories.
 *
 * <p>A step factory wraps user-provided database operations as durable DBOS workflow steps. Before
 * executing a step, the factory checks {@code tx_step_outputs} for a prior result. If one exists,
 * it is returned directly (idempotency / crash recovery). If not, the operation runs inside a
 * database transaction: the result is written to {@code tx_step_outputs} atomically with the user's
 * data, and the transaction is committed. On failure the transaction is rolled back and the error
 * is recorded so it can be replayed on retry.
 *
 * <p>Subclasses provide the connection-management primitives ({@link #openTransaction()}, {@link
 * #commit}, {@link #rollback}, {@link #close}) and SQL execution ({@link #checkExecution}, {@link
 * #recordResult}) for a specific database access layer. The type parameter {@code C} represents the
 * connection/session object that the user's lambda receives.
 *
 * <p>All implementations require a PostgreSQL datasource. Use {@link #ensurePostgres} during
 * construction to fail fast if a non-PostgreSQL datasource is supplied.
 *
 * @param <C> the connection type exposed to user lambdas (e.g. {@code Connection}, {@code Handle},
 *     {@code DSLContext})
 */
public abstract class PostgresStepFactory<C> {

  private static final Logger DDL_LOGGER = LoggerFactory.getLogger(PostgresStepFactory.class);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  protected final DBOS dbos;
  protected final String schema;
  protected final DBOSSerializer serializer;

  public static final String CHECK_SQL_TEMPLATE =
      """
      SELECT output, error, serialization
      FROM "%s".tx_step_outputs
      WHERE workflow_id = ? AND step_id = ?
      """;

  public static final String UPSERT_SQL_TEMPLATE =
      """
      INSERT INTO "%s".tx_step_outputs
        (workflow_id, step_id, output, error, serialization)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT DO NOTHING
      """;

  /**
   * Verifies that the given connection is to a PostgreSQL database.
   *
   * @throws IllegalArgumentException if the database is not PostgreSQL
   * @throws SQLException if database metadata cannot be read
   */
  public static void ensurePostgres(Connection conn) throws SQLException {
    var productName = conn.getMetaData().getDatabaseProductName();
    if (!productName.equalsIgnoreCase("PostgreSQL")) {
      throw new IllegalArgumentException(
          "PostgresStepFactory requires a PostgreSQL datasource, got: " + productName);
    }
  }

  /**
   * Returns {@code true} if the named schema exists in the database.
   *
   * @throws SQLException if the query fails
   */
  public static boolean schemaExists(Connection conn, String schema) throws SQLException {
    var sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(schema, "schema must not be null"));
      try (var rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Creates the named schema if it does not already exist.
   *
   * @throws SQLException if the DDL fails
   */
  public static void ensureSchema(Connection conn, String schema) throws SQLException {
    Objects.requireNonNull(schema, "schema must not be null");
    if (!schemaExists(conn, schema)) {
      try (var stmt = conn.createStatement()) {
        stmt.execute("CREATE SCHEMA IF NOT EXISTS \"%s\"".formatted(schema));
      }
    }
  }

  /**
   * Returns {@code true} if the {@code tx_step_outputs} table exists in the named schema.
   *
   * @throws SQLException if the query fails
   */
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

  /**
   * Creates the {@code tx_step_outputs} table in the named schema if it does not already exist.
   *
   * @throws SQLException if the DDL fails
   */
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

  /**
   * Constructs a factory for the given DBOS instance.
   *
   * @param dbos the DBOS runtime
   * @param rawSchema the schema for {@code tx_step_outputs}, or {@code null} to use the schema from
   *     the DBOS config
   * @param rawSerializer the serializer for step outputs, or {@code null} to use the serializer
   *     from the DBOS config
   */
  protected PostgresStepFactory(
      DBOS dbos, @Nullable String rawSchema, @Nullable DBOSSerializer rawSerializer) {
    this.dbos = Objects.requireNonNull(dbos);
    var config = dbos.integration().config();
    this.schema =
        SystemDatabase.sanitizeSchema(rawSchema == null ? config.databaseSchema() : rawSchema);
    this.serializer = rawSerializer == null ? config.serializer() : rawSerializer;
  }

  /** Opens a connection with a transaction already started (autoCommit off). */
  protected abstract C openTransaction();

  /** Opens a connection without starting a transaction (used for recording errors). */
  protected abstract C openConnection();

  /** Commits the transaction on the given connection. */
  protected abstract void commit(C conn);

  /** Rolls back the transaction on the given connection. */
  protected abstract void rollback(C conn);

  /** Closes and releases the given connection. */
  protected abstract void close(C conn);

  /**
   * Checks {@code tx_step_outputs} for a prior result for the given workflow step.
   *
   * @return the prior result, or {@code null} if the step has not been executed
   */
  protected abstract @Nullable StepResult checkExecution(
      String workflowId, int stepId, String stepName);

  /**
   * Writes a step result to {@code tx_step_outputs} using the given connection. Exactly one of
   * {@code output} and {@code error} must be non-null.
   */
  protected abstract void recordResult(
      C conn, String workflowId, int stepId, String output, String error, String serialization);

  /**
   * Executes a transactional step that returns a value.
   *
   * <p>On the first call for a given {@code (workflowId, stepId)}, the step function is invoked
   * inside a database transaction. The return value is serialized and written to {@code
   * tx_step_outputs} atomically with the user's data before the transaction commits. On subsequent
   * calls with the same IDs (idempotency or crash recovery), the cached output is deserialized and
   * returned without re-executing the function.
   *
   * @param func the database operation to run; receives the transactional connection object
   * @param stepName a human-readable name for logging and DBOS step tracking
   * @return the value returned by {@code func}
   * @throws E if {@code func} throws, after rolling back the transaction and recording the error
   */
  public <R, E extends Exception> R txStep(ThrowingFunction<R, C, E> func, String stepName)
      throws E {
    return dbos.<R, E>runStep(() -> txStepInternal(func, stepName), stepName);
  }

  /**
   * Executes a transactional step that returns no value.
   *
   * @param func the database operation to run; receives the transactional connection object
   * @param stepName a human-readable name for logging and DBOS step tracking
   * @throws E if {@code func} throws, after rolling back the transaction and recording the error
   */
  public <E extends Exception> void txStep(ThrowingConsumer<C, E> func, String stepName) throws E {
    txStep(
        c -> {
          func.execute(c);
          return null;
        },
        stepName);
  }

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
}
