package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

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

  /** Creates a factory using the schema from the DBOS config. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource) {
    this(dbos, dataSource, null, null);
  }

  /** Creates a factory using a custom schema for {@code tx_step_outputs}. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource, String schema) {
    this(dbos, dataSource, schema, null);
  }

  /** Creates a factory using a custom serializer. */
  public JdbcStepFactory(DBOS dbos, DataSource dataSource, DBOSSerializer serializer) {
    this(dbos, dataSource, null, serializer);
  }

  /**
   * Creates a factory with a custom schema and serializer.
   *
   * <p>Connects to the database immediately to verify it is PostgreSQL and to create the {@code
   * tx_step_outputs} table in the given schema if it does not already exist.
   *
   * @param dbos the DBOS runtime instance
   * @param dataSource a DataSource connected to a PostgreSQL database
   * @param schema the PostgreSQL schema to use for {@code tx_step_outputs}; {@code null} uses the
   *     schema from {@code dbos} configuration
   * @param serializer the serializer to use for step outputs; {@code null} uses the serializer from
   *     {@code dbos} configuration
   * @throws RuntimeException if the datasource is not PostgreSQL or the schema setup fails
   */
  public JdbcStepFactory(
      DBOS dbos, DataSource dataSource, String schema, DBOSSerializer serializer) {
    super(dbos, schema, serializer, dataSource::getConnection);
    this.dataSource = Objects.requireNonNull(dataSource);
  }

  @Override
  protected Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(checkSql())) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      try (var rs = stmt.executeQuery()) {
        if (!rs.next()) return Optional.empty();
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
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Database work that runs inside a JDBC transaction and returns a result. */
  @FunctionalInterface
  public interface TransactionalFunction<R, X extends Exception> {
    R execute(Connection conn) throws X;
  }

  /**
   * Executes {@code callback} as an idempotent DBOS step inside a JDBC transaction.
   *
   * <p>If a result for this step is already recorded (e.g. on workflow retry), the callback is
   * skipped and the cached result is returned. Otherwise the callback runs inside an open
   * transaction; the output is recorded atomically with the database work so the step is
   * exactly-once on success.
   *
   * @param <R> the return type of the callback
   * @param <X> the checked exception type the callback may throw
   * @param callback the database work to perform; receives an open {@link Connection} and must not
   *     commit or close it
   * @param stepName a stable name that identifies this step within the workflow
   * @return the value returned by {@code callback}
   * @throws X if the callback throws
   */
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
        stepName);
  }

  /** Database work that runs inside a JDBC transaction without returning a result. */
  @FunctionalInterface
  public interface TransactionalRunnable<X extends Exception> {
    void execute(Connection conn) throws X;
  }

  /**
   * Executes {@code callback} as an idempotent DBOS step inside a JDBC transaction, with no return
   * value.
   *
   * <p>Behaves identically to {@link #txStep(TransactionalFunction, String)} but accepts a {@link
   * TransactionalRunnable} for callers that do not need to return a result.
   *
   * @param <X> the checked exception type the callback may throw
   * @param callback the database work to perform; receives an open {@link Connection} and must not
   *     commit or close it
   * @param stepName a stable name that identifies this step within the workflow
   * @throws X if the callback throws
   */
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
    var conn =
        safeGet(
            () -> {
              var c = ds.getConnection();
              c.setAutoCommit(false);
              return c;
            });
    try {
      var result = func.execute(conn);
      safely(conn::commit);
      return result;
    } catch (Exception e) {
      safely(conn::rollback);
      throw e;
    } finally {
      safely(conn::close);
    }
  }

  private static void safely(ThrowingRunnable<SQLException> op) {
    try {
      op.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T safeGet(ThrowingSupplier<T, SQLException> supplier) {
    try {
      return supplier.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private <R> void recordOutput(Connection conn, String workflowId, int stepId, R result) {
    var value = SerializationUtil.serializeValue(result, null, serializer);
    recordResult(conn, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  @Override
  protected void recordError(String workflowId, int stepId, Exception exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
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
    try (var stmt = conn.prepareStatement(upsertSql())) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, stepId);
      stmt.setString(3, output);
      stmt.setString(4, error);
      stmt.setString(5, serialization);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
