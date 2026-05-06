package dev.dbos.transact.jdbi;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.JdbcStepFactory;
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
public class JdbiStepFactory {

  private final DBOS dbos;
  private final Jdbi jdbi;
  private final String schema;
  private final DBOSSerializer serializer;

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
    this.dbos = dbos;
    this.jdbi = jdbi;
    var config = dbos.integration().config();
    this.schema = SystemDatabase.sanitizeSchema(schema == null ? config.databaseSchema() : schema);
    this.serializer = serializer == null ? config.serializer() : serializer;

    try {
      jdbi.useHandle(
          handle -> {
            JdbcStepFactory.ensurePostgres(handle.getConnection());
            JdbcStepFactory.ensureSchema(handle.getConnection(), this.schema);
            JdbcStepFactory.ensureTxOutputTable(handle.getConnection(), this.schema);
          });
    } catch (Exception e) {
      if (e instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(e.getMessage(), e);
    }
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
  @SuppressWarnings("unchecked")
  public <R, X extends Exception> R inStep(final HandleCallback<R, X> callback, String stepName)
      throws X {

    return dbos.<R, X>runStep(
        () -> {
          var workflowId = Objects.requireNonNull(DBOS.workflowId());
          int stepId = Objects.requireNonNull(DBOS.stepId());

          var prevResult = checkExecution(workflowId, stepId, stepName);
          if (prevResult.isPresent()) {
            return prevResult.get().<R, X>toResult(serializer);
          }

          try {
            return jdbi.inTransaction(
                h -> {
                  var result = callback.withHandle(h);
                  recordOutput(h, workflowId, stepId, result);
                  return result;
                });
          } catch (Exception e) {
            recordError(workflowId, stepId, e);
            throw (X) e;
          }
        },
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

  private Optional<StepResult> checkExecution(String workflowId, int stepId, String stepName) {
    var sql = JdbcStepFactory.CHECK_SQL_TEMPLATE.formatted(schema);
    return jdbi.withHandle(
        h ->
            h.createQuery(sql)
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

  private <X extends Exception> void recordError(String workflowId, int stepId, X exception) {
    var value = SerializationUtil.serializeError(exception, null, serializer);
    jdbi.useTransaction(
        h -> {
          recordResult(h, workflowId, stepId, null, value.serializedValue(), value.serialization());
        });
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
    var sql = JdbcStepFactory.UPSERT_SQL_TEMPLATE.formatted(schema);
    handle
        .createUpdate(sql)
        .bind(0, workflowId)
        .bind(1, stepId)
        .bind(2, output)
        .bind(3, error)
        .bind(4, serialization)
        .execute();
  }
}
