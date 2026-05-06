package dev.dbos.transact.jdbi;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.PostgresStepFactory;
import dev.dbos.transact.workflow.internal.StepResult;

import java.util.Objects;
import java.util.Optional;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;

/**
 * Runs idempotent transactional steps inside DBOS workflows using Jdbi3 {@link Handle} objects.
 *
 * <p>Construct one with a {@link Jdbi} instance pointing at a PostgreSQL database. The constructor
 * verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table if needed.
 * Lambdas passed to {@link #inStep} or {@link #useStep} receive a {@link Handle} with a transaction
 * already open; they must not call {@code commit} or {@code close} themselves.
 *
 * <pre>{@code
 * JdbiStepFactory factory = new JdbiStepFactory(dbos, Jdbi.create(dataSource));
 *
 * // inside a @Workflow method:
 * int count = factory.inStep(handle -> {
 *     return handle.createUpdate("INSERT INTO ...").execute();
 * }, "myStep");
 * }</pre>
 */
public class JdbiStepFactory extends PostgresStepFactory {

  private final Jdbi jdbi;

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
    super(dbos, schema, serializer, () -> jdbi.open().getConnection());
    this.jdbi = Objects.requireNonNull(jdbi);
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
    return runTxStep(
        (wfId, stepId) ->
            jdbi.inTransaction(
                h -> {
                  var result = callback.withHandle(h);
                  recordOutput(h, wfId, stepId, result);
                  return result;
                }),
        stepName);
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
    inStep(
        handle -> {
          callback.useHandle(handle);
          return null;
        },
        stepName);
  }

  @Override
  protected Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    return jdbi.withHandle(
        h ->
            h.createQuery(checkSql())
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
                .findFirst());
  }

  private <R> void recordOutput(Handle handle, String workflowId, int stepId, R result) {
    var value = SerializationUtil.serializeValue(result, null, serializer);
    recordResult(handle, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  @Override
  protected void recordError(String workflowId, int stepId, Exception exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    jdbi.useTransaction(
        h ->
            recordResult(
                h, workflowId, stepId, null, value.serializedValue(), value.serialization()));
  }

  private void recordResult(
      Handle handle,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    handle
        .createUpdate(upsertSql())
        .bind(0, workflowId)
        .bind(1, stepId)
        .bind(2, output)
        .bind(3, error)
        .bind(4, serialization)
        .execute();
  }
}
