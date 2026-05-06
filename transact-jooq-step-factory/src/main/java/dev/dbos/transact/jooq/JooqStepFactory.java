package dev.dbos.transact.jooq;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.PostgresStepFactoryHelpers;
import dev.dbos.transact.workflow.internal.StepResult;

import java.util.Objects;
import java.util.Optional;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.TransactionalCallable;
import org.jooq.TransactionalRunnable;

public class JooqStepFactory {

  private final DBOS dbos;
  private final DSLContext dsl;
  private final String schema;
  private final DBOSSerializer serializer;

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
    this.dbos = dbos;
    this.dsl = dsl;
    var config = dbos.integration().config();
    this.schema = SystemDatabase.sanitizeSchema(schema == null ? config.databaseSchema() : schema);
    this.serializer = serializer == null ? config.serializer() : serializer;
    try {
      dsl.connection(
          conn -> {
            PostgresStepFactoryHelpers.ensurePostgres(conn);
            PostgresStepFactoryHelpers.ensureSchema(conn, this.schema);
            PostgresStepFactoryHelpers.ensureTxOutputTable(conn, this.schema);
          });
    } catch (Exception e) {
      if (e instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(e);
    }
  }

  public <T> T txStepResult(TransactionalCallable<T> callback, String stepName) {
    return dbos.runStep(
        () -> {
          var workflowId = Objects.requireNonNull(DBOS.workflowId());
          int stepId = Objects.requireNonNull(DBOS.stepId());

          var prevResult = checkExecution(workflowId, stepId, stepName);
          if (prevResult.isPresent()) {
            return prevResult.get().toResult(serializer);
          }

          try {
            return dsl.transactionResult(
                trx -> {
                  var result = callback.run(trx);
                  recordOutput(trx, workflowId, stepId, result);
                  return result;
                });
          } catch (Exception e) {
            recordError(workflowId, stepId, e);
            throw e;
          }
        },
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

  private Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    var sql = PostgresStepFactoryHelpers.CHECK_SQL_TEMPLATE.formatted(this.schema);
    return dsl.fetchOptional(sql, workflowId, stepId)
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
    recordResult(trx, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  private <X extends Exception> void recordError(String workflowId, int stepId, X exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    dsl.transaction(
        trx ->
            recordResult(
                trx, workflowId, stepId, null, value.serializedValue(), value.serialization()));
  }

  private void recordResult(
      Configuration trx,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    if (output != null && error != null) {
      throw new IllegalArgumentException("attempted to record non null output and error result");
    }
    var sql = PostgresStepFactoryHelpers.UPSERT_SQL_TEMPLATE.formatted(schema);
    trx.dsl().execute(sql, workflowId, stepId, output, error, serialization);
  }
}
