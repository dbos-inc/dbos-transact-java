package dev.dbos.transact.jooq;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.AbstractTxStepFactory;

import java.util.Objects;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.TransactionalCallable;
import org.jooq.TransactionalRunnable;

public class JooqStepFactory extends AbstractTxStepFactory {

  private final DSLContext dsl;

  @Override
  protected <T> T withConnection(ConnectionFn<T> fn) {
    return dsl.connectionResult(conn -> fn.apply(conn));
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
        this::recordError,
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
    trx.dsl()
        .connection(
            conn ->
                upsertResult(
                    conn,
                    schema,
                    workflowId,
                    stepId,
                    value.serializedValue(),
                    null,
                    value.serialization()));
  }

  private <X extends Exception> void recordError(String workflowId, int stepId, X exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    dsl.transaction(
        trx ->
            trx.dsl()
                .connection(
                    conn ->
                        upsertResult(
                            conn,
                            schema,
                            workflowId,
                            stepId,
                            null,
                            value.serializedValue(),
                            value.serialization())));
  }
}
