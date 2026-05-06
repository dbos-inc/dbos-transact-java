package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

/**
 * A {@link PostgresStepFactoryHelpers} implementation backed by plain JDBC {@link Connection} objects.
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
      PostgresStepFactoryHelpers.ensurePostgres(conn);
      PostgresStepFactoryHelpers.ensureSchema(conn, this.schema);
      PostgresStepFactoryHelpers.ensureTxOutputTable(conn, this.schema);
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
    var sql = PostgresStepFactoryHelpers.CHECK_SQL_TEMPLATE.formatted(this.schema);
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
    recordResult(conn, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  private <X extends Exception> void recordError(String workflowId, int stepId, X exception) {
    final var value = SerializationUtil.serializeError(exception, null, serializer);
    executeTransaction(
        dataSource,
        (Connection conn) -> {
          recordResult(
              conn, workflowId, stepId, null, value.serializedValue(), value.serialization());
          return null;
        });
  }

  private void recordResult(
      Connection conn,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    if (output != null && error != null) {
      throw new IllegalArgumentException("attempted to record non null output and error result");
    }
    var sql = PostgresStepFactoryHelpers.UPSERT_SQL_TEMPLATE.formatted(schema);
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
