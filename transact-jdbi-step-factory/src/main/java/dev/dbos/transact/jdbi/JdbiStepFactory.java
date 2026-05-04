package dev.dbos.transact.jdbi;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.ThrowingConsumer;
import dev.dbos.transact.execution.ThrowingFunction;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.JdbcStepFactory;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.SQLException;
import java.util.Objects;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbiStepFactory {

  private static final Logger logger = LoggerFactory.getLogger(JdbiStepFactory.class);

  private final DBOS dbos;
  private final Jdbi jdbi;
  private final String schema;
  private final DBOSSerializer serializer;

  public JdbiStepFactory(DBOS dbos, Jdbi jdbi) {
    this(dbos, jdbi, null, null);
  }

  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, String schema) {
    this(dbos, jdbi, schema, null);
  }

  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, DBOSSerializer serializer) {
    this(dbos, jdbi, null, serializer);
  }

  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, String schema, DBOSSerializer serializer) {
    this.dbos = Objects.requireNonNull(dbos);
    this.jdbi = Objects.requireNonNull(jdbi);
    var config = dbos.integration().config();
    this.schema = SystemDatabase.sanitizeSchema(schema == null ? config.databaseSchema() : schema);
    this.serializer = serializer == null ? config.serializer() : serializer;

    try {
      jdbi.useHandle(
          handle -> {
            try {
              JdbcStepFactory.ensureSchema(handle.getConnection(), this.schema);
              JdbcStepFactory.ensureTxOutputTable(handle.getConnection(), this.schema);
            } catch (SQLException e) {
              throw new RuntimeException(e.getMessage(), e);
            }
          });
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  private @Nullable StepResult checkExecution(String workflowId, int stepId, String stepName) {
    var sql =
        """
        SELECT output, error, serialization
        FROM "%s".tx_step_outputs
        WHERE workflow_id = ? AND step_id = ?
        """
            .formatted(this.schema);
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(sql)
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
                .findFirst()
                .orElse(null));
  }

  private void recordResult(
      Handle handle,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    if (output != null && error != null) {
      throw new IllegalArgumentException("attempted to record non null output and error result");
    }

    String sql =
        """
        INSERT INTO "%s".tx_step_outputs
            (workflow_id, step_id, output, error, serialization)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """
            .formatted(schema);

    handle
        .createUpdate(sql)
        .bind(0, workflowId)
        .bind(1, stepId)
        .bind(2, output)
        .bind(3, error)
        .bind(4, serialization)
        .execute();
  }

  private <R> void recordOutput(Handle handle, String workflowId, int stepId, R retVal) {
    var value = SerializationUtil.serializeValue(retVal, null, serializer);
    recordResult(handle, workflowId, stepId, value.serializedValue(), null, value.serialization());
  }

  private <E extends Exception> void recordError(String workflowId, int stepId, E exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    jdbi.useHandle(
        handle ->
            recordResult(
                handle, workflowId, stepId, null, value.serializedValue(), value.serialization()));
  }

  private <R, E extends Exception> R txStepInternal(
      ThrowingFunction<R, Handle, E> func, String stepName) throws E {
    var workflowId = Objects.requireNonNull(DBOS.workflowId());
    int stepId = Objects.requireNonNull(DBOS.stepId());

    logger.debug(
        "txStep starting: workflowId={} stepId={} stepName={}", workflowId, stepId, stepName);
    var prevResult = this.checkExecution(workflowId, stepId, stepName);
    if (prevResult != null) {
      logger.debug(
          "txStep cache hit: workflowId={} stepId={} stepName={}", workflowId, stepId, stepName);
      return prevResult.<R, E>toResult(serializer);
    }

    logger.debug(
        "txStep executing: workflowId={} stepId={} stepName={}", workflowId, stepId, stepName);
    var handle = jdbi.open();
    try {
      handle.begin();
      var retVal = func.execute(handle);
      recordOutput(handle, workflowId, stepId, retVal);
      handle.commit();
      logger.debug(
          "txStep succeeded: workflowId={} stepId={} stepName={}", workflowId, stepId, stepName);
      return retVal;
    } catch (Exception e) {
      handle.rollback();
      recordError(workflowId, stepId, e);
      logger.debug(
          "txStep failed: workflowId={} stepId={} stepName={} error={}",
          workflowId,
          stepId,
          stepName,
          e.getMessage());
      throw e;
    } finally {
      handle.close();
    }
  }

  public <R, E extends Exception> R txStep(ThrowingFunction<R, Handle, E> func, String stepName)
      throws E {
    return dbos.<R, E>runStep(
        () -> {
          return this.txStepInternal(func, stepName);
        },
        stepName);
  }

  public <E extends Exception> void txStep(ThrowingConsumer<Handle, E> func, String stepName)
      throws E {
    txStep(
        h -> {
          func.execute(h);
          return null;
        },
        stepName);
  }
}
