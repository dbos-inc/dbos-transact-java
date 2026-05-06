package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.TransactionalRunnable.WrappedSqlException;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PostgresStepFactoryHelpers} implementation backed by plain JDBC {@link Connection}
 * objects.
 *
 * <p>Construct one with a {@link DataSource} pointing at a PostgreSQL database. The constructor
 * verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table if needed.
 * User lambdas passed to {@code txStep} receive a {@link Connection} with a transaction already
 * started; they should not call {@code commit} or {@code close} themselves.
 *
 * <pre>{@code
 * JdbcStepFactory factory = new JdbcStepFactory(dbos, dataSource);
 *
 * // inside a @Workflow method:
 * int count = factory.txStep(conn -> {
 *     try (var stmt = conn.prepareStatement("INSERT INTO ...")) { ... }
 *     return rowCount;
 * }, "myStep");
 * }</pre>
 */
public class JdbcStepFactory {

  private static final Logger logger = LoggerFactory.getLogger(JdbcStepFactory.class);

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
    logger.debug("Creating tx_step_outputs table in schema={}", schema);
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

  private final DBOS dbos;
  private final DataSource dataSource;
  private final String schema;
  private final DBOSSerializer serializer;

  /** Creates a factory using the schema from the DBOS config. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource) throws SQLException {
    this(dbos, dataSource, null, null);
  }

  /** Creates a factory using a custom schema for {@code tx_step_outputs}. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource, String schema) throws SQLException {
    this(dbos, dataSource, schema, null);
  }

  /** Creates a factory using a custom serializer. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource, DBOSSerializer serializer)
      throws SQLException {
    this(dbos, dataSource, null, serializer);
  }

  /** Creates a factory with a custom schema and serializer. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource, String schema, DBOSSerializer serializer)
      throws SQLException {
    this.dbos = dbos;
    this.dataSource = Objects.requireNonNull(dataSource);
    var config = dbos.integration().config();
    this.schema = SystemDatabase.sanitizeSchema(schema == null ? config.databaseSchema() : schema);
    this.serializer = serializer == null ? config.serializer() : serializer;

    try (var conn = dataSource.getConnection()) {
      ensurePostgres(conn);
      ensureSchema(conn, this.schema);
      ensureTxOutputTable(conn, this.schema);
    }
  }

  @FunctionalInterface
  public interface TransactionalFunction<R, X extends Exception> {
    R execute(Connection conn) throws X;
  }

  @SuppressWarnings("unchecked")
  public <R, X extends Exception> R txStep(
      final TransactionalFunction<R, X> callback, String stepName) throws X {
    return dbos.<R, X>runStep(
        () -> {
          var workflowId = Objects.requireNonNull(DBOS.workflowId());
          int stepId = Objects.requireNonNull(DBOS.stepId());

          var prevResult = checkExecution(workflowId, stepId, stepName);
          if (prevResult.isPresent()) {
            return prevResult.get().<R, X>toResult(serializer);
          }

          try {
            return executeTransaction(
                dataSource,
                c -> {
                  var result = callback.execute(c);
                  recordOutput(c, workflowId, stepId, result);
                  return result;
                });
          } catch (Exception e) {
            recordError(workflowId, stepId, e);
            throw (X) e;
          }
        },
        stepName);
  }

  @FunctionalInterface
  public interface TransactionalRunnable<X extends Exception> {
    void execute(Connection conn) throws X;
  }

  public <X extends Exception> void txStep(final TransactionalRunnable<X> callback, String stepName)
      throws X {
    txStep(
        c -> {
          callback.execute(c);
          return null;
        },
        stepName);
  }

  private static <R, X extends Exception> R executeTransaction(
      final DataSource ds, TransactionalFunction<R, X> func) throws X {
    var conn = openTransaction(ds);
    try {
      var result = func.execute(conn);
      commit(conn);
      return result;
    } catch (Exception e) {
      rollback(conn);
      throw e;
    } finally {
      close(conn);
    }
  }

  static class WrappedSqlException extends RuntimeException {
    private final SQLException wrappedException;

    public WrappedSqlException(SQLException wrappedException) {
      super(wrappedException.getMessage(), wrappedException);
      this.wrappedException = wrappedException;
    }

    public SQLException wrappedException() {
      return this.wrappedException;
    }
  }

  private static Connection openTransaction(DataSource ds) {
    try {
      var conn = ds.getConnection();
      conn.setAutoCommit(false);
      return conn;
    } catch (SQLException e) {
      throw new WrappedSqlException(e);
    }
  }

  private static void commit(Connection conn) {
    try {
      conn.commit();
    } catch (SQLException e) {
      throw new WrappedSqlException(e);
    }
  }

  private static void rollback(Connection conn) {
    try {
      conn.rollback();
    } catch (SQLException e) {
      throw new WrappedSqlException(e);
    }
  }

  private static void close(Connection conn) {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new WrappedSqlException(e);
    }
  }

  private Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    var sql = CHECK_SQL_TEMPLATE.formatted(this.schema);
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return Optional.of(
              new StepResult(
                  workflowId,
                  stepId,
                  stepName,
                  rs.getString("output"),
                  rs.getString("error"),
                  null,
                  rs.getString("serialization")));
        }
        return Optional.empty();
      }
    } catch (SQLException e) {
      throw new WrappedSqlException(e);
    }
  }

  private <R> void recordOutput(Connection conn, String workflowId, int stepId, R result) {
    var value = SerializationUtil.serializeValue(result, null, serializer);
    recordResult(
        conn, schema, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  private <X extends Exception> void recordError(String workflowId, int stepId, X exception) {
    final var value = SerializationUtil.serializeError(exception, null, serializer);
    executeTransaction(
        dataSource,
        (Connection conn) -> {
          recordResult(
              conn,
              schema,
              workflowId,
              stepId,
              null,
              value.serializedValue(),
              value.serialization());
          return null;
        });
  }

  public static void recordResult(
      Connection conn,
      String schema,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    if (output != null && error != null) {
      throw new IllegalArgumentException("attempted to record non null output and error result");
    }
    var sql = UPSERT_SQL_TEMPLATE.formatted(schema);
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      stmt.setString(3, output);
      stmt.setString(4, error);
      stmt.setString(5, serialization);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new WrappedSqlException(e);
    }
  }
}
