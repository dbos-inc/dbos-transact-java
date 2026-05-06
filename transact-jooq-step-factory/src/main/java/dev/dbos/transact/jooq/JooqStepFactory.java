package dev.dbos.transact.jooq;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.PostgresStepFactory;
import dev.dbos.transact.workflow.internal.StepResult;

import java.util.Objects;
import java.util.Optional;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.TransactionalCallable;
import org.jooq.TransactionalRunnable;

public class JooqStepFactory extends PostgresStepFactory {

  private final DSLContext dsl;

  @Override
  protected Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    return dsl.fetchOptional(checkSql(), workflowId, stepId)
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

  /** Creates a factory with a custom schema and serializer. */
  public JooqStepFactory(DBOS dbos, DSLContext dsl, String schema, DBOSSerializer serializer) {
    super(dbos, schema, serializer, () -> dsl.configuration().connectionProvider().acquire());
    this.dsl = Objects.requireNonNull(dsl);
  }

  public <T> T txStepResult(TransactionalCallable<T> callback, String stepName) {
    return runTxStep(
        (wfId, stepId) ->
            dsl.transactionResult(
                trx -> {
                  var result = callback.run(trx);
                  recordOutput(trx, wfId, stepId, result);
                  return result;
                }),
        stepName);
  }

  public void txStep(TransactionalRunnable transactional, String stepName) {
    txStepResult(
        c -> {
          transactional.run(c);
          return null;
        },
        stepName);
  }

  private <R> void recordOutput(Configuration trx, String workflowId, int stepId, R result) {
    var value = SerializationUtil.serializeValue(result, null, serializer);
    recordResult(
        trx.dsl(), workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  @Override
  protected void recordError(String workflowId, int stepId, Exception exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    dsl.transaction(
        trx ->
            recordResult(
                trx.dsl(),
                workflowId,
                stepId,
                null,
                value.serializedValue(),
                value.serialization()));
  }

  private void recordResult(
      DSLContext ctx,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    ctx.execute(upsertSql(), workflowId, stepId, output, error, serialization);
  }
}
