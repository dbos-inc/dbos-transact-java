package dev.dbos.transact.jooq;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.PostgresStepFactory;
import dev.dbos.transact.txstep.TxStepSchema;
import dev.dbos.transact.workflow.internal.StepResult;

import java.util.Objects;
import java.util.Optional;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.TransactionalCallable;
import org.jooq.TransactionalRunnable;
import org.jooq.exception.DataAccessException;

/**
 * Runs idempotent transactional steps inside DBOS workflows using jOOQ {@link DSLContext} objects.
 *
 * <p>Construct one with a {@link DSLContext} connected to a PostgreSQL database. The constructor
 * verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table if needed.
 * Lambdas passed to {@link #txStepResult} or {@link #txStep} receive a jOOQ {@link
 * org.jooq.Configuration} with a transaction already open; they must not commit or close the
 * underlying connection themselves.
 *
 * <pre>{@code
 * JooqStepFactory factory = new JooqStepFactory(dbos, dslContext);
 *
 * // inside a @Workflow method:
 * int count = factory.txStepResult(trx -> {
 *     return trx.dsl().insertInto(...).execute();
 * }, "myStep");
 * }</pre>
 */
public class JooqStepFactory extends PostgresStepFactory {

  private final DSLContext dsl;

  /** Creates a factory using the schema from the DBOS config. */
  public JooqStepFactory(DBOS dbos, DSLContext dsl) {
    this(dbos, dsl, null, null);
  }

  /** Creates a factory using a custom schema for {@code tx_step_outputs}. */
  public JooqStepFactory(DBOS dbos, DSLContext dsl, String schema) {
    this(dbos, dsl, schema, null);
  }

  /** Creates a factory using a custom serializer. */
  public JooqStepFactory(DBOS dbos, DSLContext dsl, DBOSSerializer serializer) {
    this(dbos, dsl, null, serializer);
  }

  /**
   * Creates a factory with a custom schema and serializer.
   *
   * <p>Connects to the database immediately to verify it is PostgreSQL and to create the {@code
   * tx_step_outputs} table in the given schema if it does not already exist.
   *
   * @param dbos the DBOS runtime instance
   * @param dsl a DSLContext connected to a PostgreSQL database
   * @param schema the PostgreSQL schema to use for {@code tx_step_outputs}; {@code null} uses the
   *     schema from {@code dbos} configuration
   * @param serializer the serializer to use for step outputs; {@code null} uses the serializer from
   *     {@code dbos} configuration
   * @throws RuntimeException if the datasource is not PostgreSQL or the schema setup fails
   */
  public JooqStepFactory(DBOS dbos, DSLContext dsl, String schema, DBOSSerializer serializer) {
    super(dbos, schema, serializer, () -> dsl.configuration().connectionProvider().acquire());
    this.dsl = Objects.requireNonNull(dsl);
  }

  /**
   * Executes {@code callback} as an idempotent DBOS step inside a jOOQ transaction.
   *
   * <p>If a result for this step is already recorded (e.g. on workflow retry), the callback is
   * skipped and the cached result is returned. Otherwise the callback runs inside an open
   * transaction; the output is recorded atomically with the database work so the step is
   * exactly-once on success.
   *
   * @param <T> the return type of the callback
   * @param callback the database work to perform; receives a jOOQ {@link org.jooq.Configuration}
   *     with an open transaction and must not commit or close the underlying connection
   * @param stepName a stable name that identifies this step within the workflow
   * @return the value returned by {@code callback}
   */
  public <T> T txStepResult(TransactionalCallable<T> callback, String stepName) {
    return runTxStep(
        (wfId, stepId) -> {
          try {
            return dsl.transactionResult(
                trx -> {
                  var result = callback.run(trx);
                  recordOutput(trx, wfId, stepId, result);
                  return result;
                });
          } catch (StepConflictException e) {
            return checkExecution(wfId, stepId, stepName)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Conflict on tx_step_outputs but winner row not found: workflowId=%s stepId=%d stepName=%s"
                                .formatted(wfId, stepId, stepName),
                            e))
                .<T, RuntimeException>toResult(serializer);
          }
        },
        stepName);
  }

  /**
   * Executes {@code transactional} as an idempotent DBOS step inside a jOOQ transaction, with no
   * return value.
   *
   * <p>Behaves identically to {@link #txStepResult} but accepts a {@link TransactionalRunnable} for
   * callers that do not need to return a result.
   *
   * @param transactional the database work to perform; receives a jOOQ {@link
   *     org.jooq.Configuration} with an open transaction and must not commit or close the
   *     underlying connection
   * @param stepName a stable name that identifies this step within the workflow
   */
  public void txStep(TransactionalRunnable transactional, String stepName) {
    txStepResult(
        c -> {
          transactional.run(c);
          return null;
        },
        stepName);
  }

  @Override
  protected Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    return dsl.fetchOptional(TxStepSchema.checkSql(schema), workflowId, stepId)
        .map(
            r ->
                new StepResult(
                    workflowId,
                    stepId,
                    stepName,
                    r.get("output", String.class),
                    r.get("error", String.class),
                    null,
                    r.get("serialization", String.class)));
  }

  private <R> void recordOutput(Configuration trx, String workflowId, int stepId, R result) {
    var value = SerializationUtil.serializeValue(result, null, serializer);
    recordResult(
        trx.dsl(), workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  @Override
  protected void recordError(String workflowId, int stepId, Exception exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    try {
      dsl.transaction(
          trx ->
              recordResult(
                  trx.dsl(),
                  workflowId,
                  stepId,
                  null,
                  value.serializedValue(),
                  value.serialization()));
    } catch (StepConflictException ignored) {
    }
  }

  private void recordResult(
      DSLContext ctx,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    try {
      ctx.execute(TxStepSchema.upsertSql(schema), workflowId, stepId, output, error, serialization);
    } catch (DataAccessException e) {
      if (isUniqueViolation(e)) throw new StepConflictException(e);
      throw e;
    }
  }
}
