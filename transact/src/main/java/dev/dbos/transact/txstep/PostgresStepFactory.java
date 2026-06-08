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

  protected abstract Optional<StepResult> checkExecution(
      String workflowId, int stepId, String stepName);

  protected abstract void recordError(String workflowId, int stepId, Exception exception);

  @FunctionalInterface
  protected interface TxStepFunction<R, X extends Exception> {
    R execute(String workflowId, int stepId) throws X;
  }

  public static final class StepConflictException extends RuntimeException {
    public StepConflictException(Exception cause) {
      super(cause);
    }
  }

  public static boolean isUniqueViolation(Exception e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof SQLException sq && "23505".equals(sq.getSQLState())) return true;
    }
    return false;
  }

  public static boolean isSerializationFailure(Exception e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof SQLException sq) {
        var state = sq.getSQLState();
        if ("40001".equals(state) || "40P01".equals(state)) return true;
      }
    }
    return false;
  }

  private static final long RETRY_WAIT_INITIAL_MS = 1L;
  private static final double RETRY_BACKOFF_FACTOR = 1.5;
  private static final long RETRY_WAIT_MAX_MS = 2000L;

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

          long retryWaitMs = RETRY_WAIT_INITIAL_MS;
          while (true) {
            try {
              return execute.execute(workflowId, stepId);
            } catch (Exception e) {
              if (isSerializationFailure(e)) {
                try {
                  Thread.sleep(retryWaitMs);
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                }
                retryWaitMs =
                    Math.min((long) (retryWaitMs * RETRY_BACKOFF_FACTOR), RETRY_WAIT_MAX_MS);
                continue;
              }
              recordError(workflowId, stepId, e);
              throw (X) e;
            }
          }
        },
        stepName);
  }
}
