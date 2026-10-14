package dev.dbos.transact.txstep;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.database.SqlTransaction;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.internal.StepCheckpointStore;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;

/**
 * Abstract base for transactional step factories backed by a PostgreSQL database.
 *
 * <p>Subclasses provide a database-library-specific public API (e.g. plain JDBC {@link Connection},
 * JDBI {@code Handle}, jOOQ {@code DSLContext}) while this class owns the shared step lifecycle:
 * idempotency checking, error recording, and the {@link #runTxStep} template method that integrates
 * with the DBOS runtime.
 *
 * <p>The constructor verifies that the datasource is PostgreSQL and creates the {@code
 * tx_step_outputs} table (and its enclosing schema) if they do not already exist. It then registers
 * the factory's checkpoints with DBOS, which must not be launched yet, so DBOS can delete them when
 * it discards a workflow's history.
 *
 * <p>Subclasses: the factory is registered before the subclass constructor finishes, and stays
 * registered even if that constructor then throws, so a subclass constructor should not fail after
 * calling {@code super}.
 */
public abstract class PostgresStepFactory {

  protected final DBOS dbos;
  protected final String schema;
  protected final DBOSSerializer serializer;
  private final ConnectionOpener opener;
  // The store DBOS calls. A private class rather than this factory implementing the interface, so
  // the interface's methods do not become public methods of every factory.
  private final StepCheckpointStore checkpointStore = new CheckpointStore();

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
    this.opener = opener;

    try (var conn = opener.open()) {
      TxStepSchema.verifyPostgres(conn);
      TxStepSchema.createTable(conn, this.schema);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    dbos.integration().registerStepCheckpointStore(checkpointStore);
  }

  private final class CheckpointStore implements StepCheckpointStore {
    @Override
    public void deleteCheckpoints(String workflowId, int fromStepId) throws SQLException {
      PostgresStepFactory.this.deleteCheckpoints(workflowId, fromStepId);
    }

    @Override
    public void deleteCheckpoints(Collection<String> workflowIds) throws SQLException {
      if (!workflowIds.isEmpty()) {
        PostgresStepFactory.this.deleteCheckpoints(workflowIds);
      }
    }

    @Override
    public void deleteCompletedCheckpoints(String workflowId) throws SQLException {
      PostgresStepFactory.this.deleteCompletedCheckpoints(workflowId);
    }
  }

  /**
   * Deletes a workflow's checkpoints from {@code fromStepId} on, in a transaction of its own that
   * commits before this returns. DBOS calls it when it discards the workflow's history.
   *
   * <p>This default opens a connection and commits explicitly. Subclasses override it to go through
   * their database library's own transaction handling.
   */
  protected void deleteCheckpoints(String workflowId, int fromStepId) throws SQLException {
    try (var conn = opener.open()) {
      SqlTransaction.run(conn, c -> TxStepSchema.deleteFromStep(c, schema, workflowId, fromStepId));
    }
  }

  /**
   * Deletes every checkpoint of the given workflows, never empty, in a transaction of its own that
   * commits before this returns. DBOS calls it when it deletes the workflows.
   *
   * <p>This default opens a connection and commits explicitly. Subclasses override it to go through
   * their database library's own transaction handling.
   */
  protected void deleteCheckpoints(Collection<String> workflowIds) throws SQLException {
    try (var conn = opener.open()) {
      SqlTransaction.run(conn, c -> TxStepSchema.deleteWorkflows(c, schema, workflowIds));
    }
  }

  /**
   * Deletes every checkpoint of a workflow whose execution on this thread has just finished, in a
   * transaction of its own: delete them all, then {@link #checkStillOwned(String, Connection)} on
   * the same connection, then commit. DBOS calls it when the workflow's execution finishes, before
   * recording its outcome; when the execution no longer owns the workflow, the check throws and the
   * delete rolls back.
   *
   * <p>This default opens a connection and commits explicitly. Subclasses override it to go through
   * their database library's own transaction handling.
   */
  protected void deleteCompletedCheckpoints(String workflowId) throws SQLException {
    try (var conn = opener.open()) {
      SqlTransaction.run(
          conn,
          c -> {
            TxStepSchema.deleteFromStep(c, schema, workflowId, 0);
            checkStillOwned(workflowId, c);
          });
    }
  }

  protected abstract Optional<StepResult> checkExecution(
      String workflowId, int stepId, String stepName);

  /**
   * The pool this factory's connections come from, when the factory can tell; null otherwise. When
   * it is the pool the system database uses, the ownership check runs on the step's own connection
   * instead of taking a second one from that pool.
   */
  protected @Nullable DataSource dataSource() {
    return null;
  }

  /**
   * Throws {@link DBOSWorkflowExecutionConflictException} unless this step's workflow execution
   * still owns the workflow. Subclasses call it inside the step's transaction, on its connection,
   * after recording the result and before committing, so an execution that has lost its workflow
   * commits nothing.
   *
   * @throws OwnershipUnknownException if ownership could not be checked at all
   */
  protected void checkStillOwned(String workflowId, Connection conn) {
    checkStillOwned(dbos, workflowId, dataSource(), conn);
  }

  /**
   * Thrown in place of a failure to check ownership -- the system database unreachable, or DBOS
   * shutting down -- so a step's failure handling can tell it from the step's own failure. It says
   * nothing about the step, so it is never recorded as the step's result.
   */
  public static final class OwnershipUnknownException extends RuntimeException {
    OwnershipUnknownException(RuntimeException cause) {
      super(cause);
    }
  }

  /**
   * As {@link #checkStillOwned(String, Connection)}, for a factory that does not extend this class.
   *
   * @param dataSource the pool {@code conn} came from, if known
   * @param conn the step's connection, or null outside its transaction
   * @throws DBOSWorkflowExecutionConflictException if the execution no longer owns the workflow
   * @throws OwnershipUnknownException if ownership could not be checked at all
   */
  public static void checkStillOwned(
      DBOS dbos, String workflowId, @Nullable DataSource dataSource, @Nullable Connection conn) {
    try {
      dbos.integration().checkStillOwned(workflowId, dataSource, conn);
    } catch (DBOSWorkflowExecutionConflictException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new OwnershipUnknownException(e);
    }
  }

  /**
   * What a step's failure handling throws in place of {@code failure} without recording it: the
   * cause of an {@link OwnershipUnknownException} anywhere in its chain, which is the real failure
   * and not the step's, or a {@link DBOSWorkflowExecutionConflictException}. Null when {@code
   * failure} is the step's own.
   */
  public static @Nullable RuntimeException notTheStepsFailure(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof OwnershipUnknownException unknown) {
        return (RuntimeException) unknown.getCause();
      }
      if (t instanceof DBOSWorkflowExecutionConflictException conflict) {
        return conflict;
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return null;
  }

  /**
   * Whether a step's own failure may be recorded as its result: only while its execution still owns
   * the workflow. A stale execution's failure is not the owner's to replay, and when ownership
   * cannot be checked, nothing is recorded; the step's outcome is then settled when it is recorded
   * in the system database, which is fenced.
   */
  public static boolean mayRecordFailure(DBOS dbos, String workflowId) {
    try {
      checkStillOwned(dbos, workflowId, null, null);
      return true;
    } catch (DBOSWorkflowExecutionConflictException | OwnershipUnknownException e) {
      return false;
    }
  }

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

  /**
   * Whether a failure is a transaction conflict: SQLSTATE 40001 serialization_failure or 40P01
   * deadlock_detected.
   *
   * <p>This is the public entry point for the predicate; it delegates to the internal one the
   * system database retries by, so the two cannot drift.
   *
   * @param e the failure to classify, at any depth in its cause chain
   * @return whether the database rolled the transaction back for a conflict
   */
  public static boolean isSerializationFailure(Exception e) {
    return SystemDatabase.isSerializationError(e);
  }

  /**
   * The waits between retries of a step whose transaction failed with a conflict ({@link
   * #isSerializationFailure}): 1 ms, growing 1.5x per retry up to 2 s. The number of retries is not
   * bounded. Create one per step.
   */
  public static final class RetryBackoff {
    private static final long INITIAL_WAIT_NANOS = 1_000_000L;
    private static final double BACKOFF_FACTOR = 1.5;
    private static final long MAX_WAIT_NANOS = 2_000_000_000L;

    // Kept as a double so the growth is not lost to truncation while the wait is small.
    private double waitNanos = INITIAL_WAIT_NANOS;

    /**
     * Returns the wait before the next retry and advances to the one after it.
     *
     * @return the wait before the next retry
     */
    public Duration nextWait() {
      var wait = Duration.ofNanos(Math.round(waitNanos));
      waitNanos = Math.min(waitNanos * BACKOFF_FACTOR, MAX_WAIT_NANOS);
      return wait;
    }

    /**
     * Sleeps for {@link #nextWait()}.
     *
     * @throws RuntimeException if the thread is interrupted; the interrupt flag is set again
     */
    public void sleep() {
      var wait = nextWait();
      try {
        Thread.sleep(wait.toMillis(), wait.toNanosPart() % 1_000_000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted during serialization retry backoff", ie);
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected <R, X extends Exception> R runTxStep(TxStepFunction<R, X> execute, String stepName)
      throws X {
    // Before the step, so a replayed step counts too: this execution then also clears the rows an
    // earlier one wrote.
    dbos.integration().useStepCheckpointStore(checkpointStore);
    return dbos.<R, X>runStep(
        () -> {
          var workflowId = Objects.requireNonNull(DBOS.workflowId());
          int stepId = Objects.requireNonNull(DBOS.stepId());

          // A replay needs no ownership check of its own: the system database records this step,
          // and that record is fenced.
          var prev = checkExecution(workflowId, stepId, stepName);
          if (prev.isPresent()) {
            return prev.get().<R, X>toResult(serializer);
          }

          var backoff = new RetryBackoff();
          while (true) {
            try {
              return execute.execute(workflowId, stepId);
            } catch (Exception e) {
              if (SystemDatabase.isSerializationError(e)) {
                backoff.sleep();
                continue;
              }
              var notTheSteps = notTheStepsFailure(e);
              if (notTheSteps != null) {
                throw notTheSteps;
              }
              if (mayRecordFailure(dbos, workflowId)) {
                recordError(workflowId, stepId, e);
              }
              throw (X) e;
            }
          }
        },
        stepName);
  }
}
