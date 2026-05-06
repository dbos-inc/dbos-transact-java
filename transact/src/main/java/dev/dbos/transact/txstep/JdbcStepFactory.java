package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

/**
 * A StepFactory implementation backed by plain JDBC {@link Connection} objects.
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
public class JdbcStepFactory extends PostgresStepFactory {

  private final DataSource dataSource;

  @Override
  protected <T> T withConnection(ConnectionFn<T> fn) {
    try (var conn = dataSource.getConnection()) {
      return fn.apply(conn);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

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
    super(dbos, schema, serializer, dataSource::getConnection);
    this.dataSource = Objects.requireNonNull(dataSource);
  }

  @FunctionalInterface
  public interface TransactionalFunction<R, X extends Exception> {
    R execute(Connection conn) throws X;
  }

  public <R, X extends Exception> R txStep(
      final TransactionalFunction<R, X> callback, String stepName) throws X {
    return runTxStep(
        (wfId, stepId) ->
            executeTransaction(
                dataSource,
                c -> {
                  var result = callback.execute(c);
                  recordOutput(c, wfId, stepId, result);
                  return result;
                }),
        this::recordError,
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

  private static Connection openTransaction(DataSource ds) {
    try {
      var conn = ds.getConnection();
      conn.setAutoCommit(false);
      return conn;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void commit(Connection conn) {
    try {
      conn.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void rollback(Connection conn) {
    try {
      conn.rollback();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void close(Connection conn) {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
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
    upsertResult(conn, schema, workflowId, stepId, output, error, serialization);
  }
}
