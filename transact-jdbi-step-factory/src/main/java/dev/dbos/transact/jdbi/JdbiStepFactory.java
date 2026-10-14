package dev.dbos.transact.jdbi;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.PostgresStepFactory;
import dev.dbos.transact.txstep.TxStepSchema;
import dev.dbos.transact.workflow.internal.StepResult;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.jspecify.annotations.Nullable;

/**
 * Runs idempotent transactional steps inside DBOS workflows using Jdbi3 {@link Handle} objects.
 *
 * <p>Construct one with a {@link Jdbi} instance pointing at a PostgreSQL database. The constructor
 * verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table if needed.
 * Lambdas passed to {@link #inStep} or {@link #useStep} receive a {@link Handle} with a transaction
 * already open; they must not call {@code commit} or {@code close} themselves.
 *
 * <p>Create the factory before calling {@link dev.dbos.transact.DBOS#launch()}: DBOS needs to know
 * every factory when it launches, so the constructor throws {@link IllegalStateException} after
 * launch.
 *
 * <p>Pass the pool the {@link Jdbi} was created from as well, especially when it may be the pool
 * the DBOS system database uses; see {@link #JdbiStepFactory(DBOS, Jdbi, DataSource)}.
 *
 * <pre>{@code
 * JdbiStepFactory factory = new JdbiStepFactory(dbos, Jdbi.create(dataSource), dataSource);
 *
 * // inside a @Workflow method:
 * int count = factory.inStep(handle -> {
 *     return handle.createUpdate("INSERT INTO ...").execute();
 * }, "myStep");
 * }</pre>
 */
public class JdbiStepFactory extends PostgresStepFactory {

  private final Jdbi jdbi;
  // The pool jdbi was created from, when the caller passed it; Jdbi itself does not expose it.
  private final @Nullable DataSource dataSource;

  /**
   * Creates a factory using the schema and serializer from {@code dbos} configuration.
   *
   * @param dbos the DBOS runtime instance
   * @param jdbi a Jdbi instance connected to a PostgreSQL database
   */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi) {
    this(dbos, jdbi, null, null);
  }

  /**
   * Creates a factory using the given schema and the serializer from {@code dbos} configuration.
   *
   * @param dbos the DBOS runtime instance
   * @param jdbi a Jdbi instance connected to a PostgreSQL database
   * @param schema the PostgreSQL schema to use for {@code tx_step_outputs}; {@code null} uses the
   *     schema from {@code dbos} configuration
   */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, String schema) {
    this(dbos, jdbi, schema, null);
  }

  /**
   * Creates a factory using the given serializer and the schema from {@code dbos} configuration.
   *
   * @param dbos the DBOS runtime instance
   * @param jdbi a Jdbi instance connected to a PostgreSQL database
   * @param serializer the serializer to use for step outputs; {@code null} uses the serializer from
   *     {@code dbos} configuration
   */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, DBOSSerializer serializer) {
    this(dbos, jdbi, null, serializer);
  }

  /**
   * Creates a factory with explicit schema and serializer overrides.
   *
   * <p>Connects to the database immediately to verify it is PostgreSQL and to create the {@code
   * tx_step_outputs} table in the given schema if it does not already exist.
   *
   * @param dbos the DBOS runtime instance
   * @param jdbi a Jdbi instance connected to a PostgreSQL database
   * @param schema the PostgreSQL schema to use for {@code tx_step_outputs}; {@code null} uses the
   *     schema from {@code dbos} configuration
   * @param serializer the serializer to use for step outputs; {@code null} uses the serializer from
   *     {@code dbos} configuration
   * @throws RuntimeException if the datasource is not PostgreSQL or the schema setup fails
   */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, String schema, DBOSSerializer serializer) {
    this(dbos, jdbi, null, schema, serializer);
  }

  /**
   * Creates a factory that knows the pool its {@link Jdbi} was created from, using the schema and
   * serializer from {@code dbos} configuration.
   *
   * <p>Pass the pool when it may be the one the DBOS system database uses: DBOS then checks that a
   * step's workflow execution still owns the workflow on the step's own connection, rather than
   * taking a second connection from the same pool while the step holds one. Jdbi does not expose
   * the pool it was created from, so the factory cannot find it otherwise.
   *
   * @param dbos the DBOS runtime instance
   * @param jdbi a Jdbi instance connected to a PostgreSQL database
   * @param dataSource the pool {@code jdbi} was created from, as in {@code Jdbi.create(dataSource)}
   */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, DataSource dataSource) {
    this(dbos, jdbi, dataSource, null, null);
  }

  /**
   * Creates a factory that knows the pool its {@link Jdbi} was created from, with explicit schema
   * and serializer overrides. See {@link #JdbiStepFactory(DBOS, Jdbi, DataSource)} for why the pool
   * matters.
   *
   * @param dbos the DBOS runtime instance
   * @param jdbi a Jdbi instance connected to a PostgreSQL database
   * @param dataSource the pool {@code jdbi} was created from; {@code null} when it is not known
   * @param schema the PostgreSQL schema to use for {@code tx_step_outputs}; {@code null} uses the
   *     schema from {@code dbos} configuration
   * @param serializer the serializer to use for step outputs; {@code null} uses the serializer from
   *     {@code dbos} configuration
   * @throws RuntimeException if the datasource is not PostgreSQL or the schema setup fails
   */
  public JdbiStepFactory(
      DBOS dbos,
      Jdbi jdbi,
      @Nullable DataSource dataSource,
      String schema,
      DBOSSerializer serializer) {
    super(dbos, schema, serializer, () -> jdbi.open().getConnection());
    this.jdbi = Objects.requireNonNull(jdbi);
    this.dataSource = dataSource;
  }

  @Override
  protected @Nullable DataSource dataSource() {
    return dataSource;
  }

  /**
   * Executes {@code callback} as an idempotent DBOS step inside a Jdbi transaction.
   *
   * <p>If a result for this step is already recorded (e.g. on workflow retry), the callback is
   * skipped and the cached result is returned. Otherwise the callback runs inside an open
   * transaction; the output is recorded atomically with the database work so the step is
   * exactly-once on success.
   *
   * @param <R> the return type of the callback
   * @param <X> the checked exception type the callback may throw
   * @param callback the database work to perform; receives an open {@link Handle} and must not
   *     commit or close it
   * @param stepName a stable name that identifies this step within the workflow
   * @return the value returned by {@code callback}
   * @throws X if the callback throws
   */
  public <R, X extends Exception> R inStep(final HandleCallback<R, X> callback, String stepName)
      throws X {
    return inStep(callback, new JdbiStepOptions(stepName));
  }

  /**
   * Executes {@code callback} as an idempotent DBOS step inside a Jdbi transaction.
   *
   * <p>Behaves identically to {@link #inStep(HandleCallback, String)} but accepts a {@link
   * JdbiStepOptions} to control the step name and transaction isolation level.
   *
   * @param <R> the return type of the callback
   * @param <X> the checked exception type the callback may throw
   * @param callback the database work to perform; receives an open {@link Handle} and must not
   *     commit or close it
   * @param options step name and optional isolation level override
   * @return the value returned by {@code callback}
   * @throws X if the callback throws
   */
  public <R, X extends Exception> R inStep(
      final HandleCallback<R, X> callback, JdbiStepOptions options) throws X {
    var jdbiLevel = options.isolationLevel();
    return runTxStep(
        (wfId, stepId) -> {
          HandleCallback<R, X> wrapped =
              h -> {
                var result = callback.withHandle(h);
                recordOutput(h, wfId, stepId, result);
                checkStillOwned(wfId, h.getConnection());
                return result;
              };
          try {
            return inOwnTransaction(jdbiLevel, wrapped);
          } catch (StepConflictException e) {
            return checkExecution(wfId, stepId, options.name())
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Conflict on tx_step_outputs but winner row not found: workflowId=%s stepId=%d stepName=%s"
                                .formatted(wfId, stepId, options.name()),
                            e))
                .<R, X>toResult(serializer);
          }
        },
        options.name());
  }

  /**
   * Executes {@code callback} as an idempotent DBOS step inside a Jdbi transaction, with no return
   * value.
   *
   * <p>Behaves identically to {@link #inStep} but accepts a {@link HandleConsumer} for callers that
   * do not need to return a result.
   *
   * @param <X> the checked exception type the callback may throw
   * @param callback the database work to perform; receives an open {@link Handle} and must not
   *     commit or close it
   * @param stepName a stable name that identifies this step within the workflow
   * @throws X if the callback throws
   */
  public <X extends Exception> void useStep(final HandleConsumer<X> callback, String stepName)
      throws X {
    useStep(callback, new JdbiStepOptions(stepName));
  }

  /**
   * Executes {@code callback} as an idempotent DBOS step inside a Jdbi transaction, with no return
   * value.
   *
   * <p>Behaves identically to {@link #inStep(HandleCallback, JdbiStepOptions)} but accepts a {@link
   * HandleConsumer} for callers that do not need to return a result.
   *
   * @param <X> the checked exception type the callback may throw
   * @param callback the database work to perform; receives an open {@link Handle} and must not
   *     commit or close it
   * @param options step name and optional isolation level override
   * @throws X if the callback throws
   */
  public <X extends Exception> void useStep(
      final HandleConsumer<X> callback, JdbiStepOptions options) throws X {
    inStep(
        handle -> {
          callback.useHandle(handle);
          return null;
        },
        options);
  }

  @Override
  protected Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    return jdbi.withHandle(
        h ->
            h.createQuery(TxStepSchema.checkSql(schema))
                .bind(0, workflowId)
                .bind(1, stepId)
                .map(
                    (rs, ctx) ->
                        new StepResult(
                            workflowId,
                            stepId,
                            stepName,
                            rs.getString("output"),
                            rs.getString("error"),
                            null,
                            rs.getString("serialization")))
                .findOne());
  }

  private <R> void recordOutput(Handle handle, String workflowId, int stepId, R result) {
    var value = SerializationUtil.serializeValue(result, null, serializer);
    recordResult(handle, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  @Override
  protected void recordError(String workflowId, int stepId, Exception exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    try {
      inOwnTransaction(
          null,
          h -> {
            recordResult(
                h, workflowId, stepId, null, value.serializedValue(), value.serialization());
            return null;
          });
    } catch (StepConflictException ignored) {
    }
  }

  @Override
  protected void deleteCheckpoints(String workflowId, int fromStepId) {
    inOwnTransaction(
        null,
        h ->
            h.createUpdate(TxStepSchema.deleteFromStepSql(schema))
                .bind(0, workflowId)
                .bind(1, fromStepId)
                .execute());
  }

  @Override
  protected void deleteCheckpoints(Collection<String> workflowIds) {
    inOwnTransaction(
        null,
        h ->
            h.createUpdate(TxStepSchema.deleteWorkflowsSql(schema))
                .bindArray(0, String.class, workflowIds)
                .execute());
  }

  // Jdbi's inTransaction and useTransaction do not commit a connection that arrives with
  // autocommit off: they take that for a transaction already in progress and leave the commit to
  // someone else. begin() and commit() on the handle commit either way.
  private <R, X extends Exception> R inOwnTransaction(
      TransactionIsolationLevel level, HandleCallback<R, X> work) throws X {
    try (var handle = jdbi.open()) {
      var previousLevel = level != null ? handle.getTransactionIsolationLevel() : null;
      if (level != null) {
        handle.setTransactionIsolationLevel(level);
      }
      R result;
      try {
        handle.begin();
        result = work.withHandle(handle);
        handle.commit();
      } catch (Throwable t) {
        // Rolling back and restoring the isolation level can themselves fail on a broken
        // connection. Suppress both onto the original throwable rather than replacing it.
        try {
          handle.rollback();
        } catch (RuntimeException rollbackFailure) {
          t.addSuppressed(rollbackFailure);
        }
        if (level != null) {
          try {
            handle.setTransactionIsolationLevel(previousLevel);
          } catch (RuntimeException restoreFailure) {
            t.addSuppressed(restoreFailure);
          }
        }
        throw t;
      }
      if (level != null) {
        handle.setTransactionIsolationLevel(previousLevel);
      }
      return result;
    }
  }

  private void recordResult(
      Handle handle,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    try {
      handle
          .createUpdate(TxStepSchema.upsertSql(schema))
          .bind(0, workflowId)
          .bind(1, stepId)
          .bind(2, output)
          .bind(3, error)
          .bind(4, serialization)
          .execute();
    } catch (UnableToExecuteStatementException e) {
      if (isUniqueViolation(e)) throw new StepConflictException(e);
      throw e;
    }
  }
}
