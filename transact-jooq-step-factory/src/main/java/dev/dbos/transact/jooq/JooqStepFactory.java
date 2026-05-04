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

public class JooqStepFactory extends PostgresStepFactory<DSLContext> {

  private final DSLContext dsl;

  public JooqStepFactory(DBOS dbos, DSLContext dsl) {
    this(dbos, dsl, null, null);
  }

  public JooqStepFactory(DBOS dbos, DSLContext dsl, String schema) {
    this(dbos, dsl, schema, null);
  }

  public JooqStepFactory(DBOS dbos, DSLContext dsl, DBOSSerializer serializer) {
    this(dbos, dsl, null, serializer);
  }

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
