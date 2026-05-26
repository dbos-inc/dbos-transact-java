package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract base for transactional step factories backed by a PostgreSQL database.
 *
 * <p>Subclasses provide a database-library-specific public API (e.g. plain JDBC {@link Connection},
 * JDBI {@code Handle}, jOOQ {@code DSLContext}) while this class owns the shared step lifecycle:
 * idempotency checking, error recording, and the {@link #runTxStep} template method that integrates
 * with the DBOS runtime.
 *
 * <p>The constructor verifies that the datasource is PostgreSQL and creates the {@code
 * tx_step_outputs} table (and its enclosing schema) if they do not already exist.
 */
public abstract class PostgresStepFactory {

  protected final DBOS dbos;
  protected final String schema;
  protected final DBOSSerializer serializer;

  @FunctionalInterface
  protected interface ConnectionOpener {
    Connection open() throws SQLException;
  }

  protected PostgresStepFactory(
      DBOS dbos, String schema, DBOSSerializer serializer, ConnectionOpener opener) {
    this.dbos = Objects.requireNonNull(dbos);
    var config = dbos.integration().config();
    this.schema = SystemDatabase.sanitizeSchema(schema == null ? config.databaseSchema() : schema);
    this.serializer = serializer == null ? config.serializer() : serializer;

    try (var conn = opener.open()) {
      TxStepSchema.verifyPostgres(conn);
      TxStepSchema.createTable(conn, this.schema);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected String checkSql() {
    return TxStepSchema.checkSql(schema);
  }

  protected abstract Optional<StepResult> checkExecution(
      String workflowId, int stepId, String stepName);

  protected String upsertSql() {
    return TxStepSchema.upsertSql(schema);
  }

  protected abstract void recordError(String workflowId, int stepId, Exception exception);

  @FunctionalInterface
  protected interface TxStepFunction<R, X extends Exception> {
    R execute(String workflowId, int stepId) throws X;
  }

  protected static final class StepConflictException extends RuntimeException {
    public StepConflictException(Exception cause) {
      super(cause);
    }
  }

  protected static boolean isUniqueViolation(Exception e) {
    if (e instanceof SQLException sq) return "23505".equals(sq.getSQLState());
    if (e.getCause() instanceof SQLException sq) return "23505".equals(sq.getSQLState());
    return false;
  }

  @SuppressWarnings("unchecked")
  protected <R, X extends Exception> R runTxStep(TxStepFunction<R, X> execute, String stepName)
      throws X {
    return dbos.<R, X>runStep(
        () -> {
          var workflowId = Objects.requireNonNull(DBOS.workflowId());
          int stepId = Objects.requireNonNull(DBOS.stepId());

          var prev = checkExecution(workflowId, stepId, stepName);
          if (prev.isPresent()) {
            return prev.get().<R, X>toResult(serializer);
          }

          try {
            return execute.execute(workflowId, stepId);
          } catch (Exception e) {
            recordError(workflowId, stepId, e);
            throw (X) e;
          }
        },
        stepName);
  }
}
