package dev.dbos.transact.jdbi;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.txstep.PostgresStepFactory;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.SQLException;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jspecify.annotations.Nullable;

/**
 * A {@link PostgresStepFactory} implementation backed by Jdbi3 {@link Handle} objects.
 *
 * <p>Construct one with a {@link Jdbi} instance pointing at a PostgreSQL database. The constructor
 * verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table if needed.
 * User lambdas passed to {@code txStep} receive a {@link Handle} with a transaction already
 * started; they should not call {@code commit} or {@code close} themselves.
 *
 * <pre>{@code
 * JdbiStepFactory factory = new JdbiStepFactory(dbos, Jdbi.create(dataSource));
 *
 * // inside a @Workflow method:
 * int count = factory.txStep(handle -> {
 *     return handle.createUpdate("INSERT INTO ...").execute();
 * }, "myStep");
 * }</pre>
 */
public class JdbiStepFactory extends PostgresStepFactory<Handle> {

  private final Jdbi jdbi;

  /** Creates a factory using the schema from the DBOS config. */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi) {
    this(dbos, jdbi, null, null);
  }

  /** Creates a factory using a custom schema for {@code tx_step_outputs}. */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, String schema) {
    this(dbos, jdbi, schema, null);
  }

  /** Creates a factory using a custom serializer. */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, DBOSSerializer serializer) {
    this(dbos, jdbi, null, serializer);
  }

  /** Creates a factory with a custom schema and serializer. */
  public JdbiStepFactory(DBOS dbos, Jdbi jdbi, String schema, DBOSSerializer serializer) {
    super(dbos, schema, serializer);
    this.jdbi = jdbi;
    try {
      jdbi.useHandle(
          handle -> {
            try {
              PostgresStepFactory.ensurePostgres(handle.getConnection());
              PostgresStepFactory.ensureSchema(handle.getConnection(), this.schema);
              PostgresStepFactory.ensureTxOutputTable(handle.getConnection(), this.schema);
            } catch (SQLException e) {
              throw new RuntimeException(e.getMessage(), e);
            }
          });
    } catch (Exception e) {
      if (e instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Handle openTransaction() {
    var handle = jdbi.open();
    handle.begin();
    return handle;
  }

  @Override
  protected Handle openConnection() {
    return jdbi.open();
  }

  @Override
  protected void commit(Handle handle) {
    handle.commit();
  }

  @Override
  protected void rollback(Handle handle) {
    handle.rollback();
  }

  @Override
  protected void close(Handle handle) {
    handle.close();
  }

  @Override
  protected @Nullable StepResult checkExecution(String workflowId, int stepId, String stepName) {
    var sql = CHECK_SQL_TEMPLATE.formatted(this.schema);
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

  @Override
  protected void recordResult(
      Handle handle,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    if (output != null && error != null) {
      throw new IllegalArgumentException("attempted to record non null output and error result");
    }
    handle
        .createUpdate(UPSERT_SQL_TEMPLATE.formatted(schema))
        .bind(0, workflowId)
        .bind(1, stepId)
        .bind(2, output)
        .bind(3, error)
        .bind(4, serialization)
        .execute();
  }
}
