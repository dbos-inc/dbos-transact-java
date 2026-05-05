package dev.dbos.transact.jooq;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.txstep.PostgresStepFactory;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jspecify.annotations.Nullable;

/**
 * A {@link PostgresStepFactory} implementation backed by jOOQ {@link DSLContext} objects.
 *
 * <p>Construct one with a pool-backed {@link DSLContext} pointing at a PostgreSQL database. The
 * constructor verifies the datasource is PostgreSQL and creates the {@code tx_step_outputs} table
 * if needed. User lambdas passed to {@code txStep} receive a per-transaction {@link DSLContext}
 * backed by a single connection with a transaction already started; they should not call {@code
 * commit} or {@code close} themselves.
 *
 * <pre>{@code
 * DSLContext dsl = DSL.using(dataSource, SQLDialect.POSTGRES);
 * JooqStepFactory factory = new JooqStepFactory(dbos, dsl);
 *
 * // inside a @Workflow method:
 * int count = factory.txStep(ctx -> {
 *     return ctx.execute("INSERT INTO ...");
 * }, "myStep");
 * }</pre>
 */
public class JooqStepFactory extends PostgresStepFactory<DSLContext> {

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

  /** Creates a factory with a custom schema and serializer. */
  public JooqStepFactory(DBOS dbos, DSLContext dsl, String schema, DBOSSerializer serializer) {
    super(dbos, schema, serializer);
    this.dsl = dsl;
    try {
      dsl.connection(
          conn -> {
            try {
              ensurePostgres(conn);
              ensureSchema(conn, this.schema);
              ensureTxOutputTable(conn, this.schema);
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
  protected DSLContext openTransaction() {
    try {
      var conn = dsl.configuration().connectionProvider().acquire();
      conn.setAutoCommit(false);
      return DSL.using(conn, SQLDialect.POSTGRES);
    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  protected DSLContext openConnection() {
    var conn = dsl.configuration().connectionProvider().acquire();
    return DSL.using(conn, SQLDialect.POSTGRES);
  }

  @Override
  protected void commit(DSLContext ctx) {
    try {
      ctx.connection(conn -> conn.commit());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  protected void rollback(DSLContext ctx) {
    try {
      ctx.connection(conn -> conn.rollback());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  protected void close(DSLContext ctx) {
    try {
      ctx.connection(conn -> conn.close());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  protected @Nullable StepResult checkExecution(String workflowId, int stepId, String stepName) {
    var sql = CHECK_SQL_TEMPLATE.formatted(this.schema);
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
                    r.get("serialization", String.class)))
        .orElse(null);
  }

  @Override
  protected void recordResult(
      DSLContext ctx,
      String workflowId,
      int stepId,
      String output,
      String error,
      String serialization) {
    if (output != null && error != null) {
      throw new IllegalArgumentException("attempted to record non null output and error result");
    }
    ctx.execute(
        UPSERT_SQL_TEMPLATE.formatted(schema), workflowId, stepId, output, error, serialization);
  }
}
