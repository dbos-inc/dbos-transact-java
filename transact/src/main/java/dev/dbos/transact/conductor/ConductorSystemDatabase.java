package dev.dbos.transact.conductor;

import java.sql.SQLException;
import java.util.Set;

import javax.sql.DataSource;

/**
 * The system database, for the Conductor-only operations {@code dev.dbos:transact-enterprise} runs
 * in SQL of its own. It is not application API, and it changes only as {@link ConductorEngine}
 * does.
 *
 * <p>It hands out the means to run SQL, not an abstraction over the tables: that SQL addresses the
 * system database's tables directly and follows DBOS Transact's migrations.
 */
public interface ConductorSystemDatabase {

  /** Database work that may fail with {@link SQLException}. */
  @FunctionalInterface
  interface SqlWork<T> {
    T run() throws SQLException;
  }

  /** The system database's connections. */
  DataSource dataSource();

  /**
   * The schema holding the system database's tables, unquoted. It never contains a double quote, so
   * it can be quoted as {@code "<schema>"} in SQL.
   */
  String schema();

  /**
   * Returns every descendant of a workflow: its children, their children, and so on. Runs once,
   * without retrying, so it composes inside {@link #retry}.
   *
   * @param workflowId the workflow
   * @return the descendants' IDs, without {@code workflowId} itself
   * @throws SQLException if the database fails
   */
  Set<String> workflowChildren(String workflowId) throws SQLException;

  /**
   * Runs {@code work}, retrying it as DBOS Transact retries its own system-database calls: on a
   * lost connection or a transient failure, with backoff, until it succeeds or DBOS shuts down. A
   * transaction conflict (SQLSTATE 40001 or 40P01) is not retried; it reaches the caller.
   *
   * @param work the work, which must be safe to run more than once
   * @return what {@code work} returns
   * @throws dev.dbos.transact.exceptions.DBOSSystemDatabaseException if the failure is not one to
   *     retry, or an interrupt stops the retries
   * @throws IllegalStateException if the system database has been closed
   */
  <T> T retry(SqlWork<T> work);

  /**
   * As {@link #retry}, but a transaction conflict is replayed too, up to ten times. A conflict
   * means a peer committed first, so the replay sees the database after that commit: {@code work}
   * must be safe to re-run against it, as a transaction of plain inserts that fails on an existing
   * row is.
   *
   * @param operation what is being run, for the log
   * @param work the work, which must be safe to replay after a peer's commit
   * @return what {@code work} returns
   * @throws dev.dbos.transact.exceptions.DBOSSystemDatabaseException if the failure is not one to
   *     retry, or an interrupt stops the retries
   * @throws IllegalStateException if the system database has been closed
   */
  <T> T retryIncludingSerializationError(String operation, SqlWork<T> work);
}
