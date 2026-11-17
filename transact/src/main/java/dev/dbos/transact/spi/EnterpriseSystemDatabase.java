package dev.dbos.transact.spi;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;

/**
 * The system database, for the operations {@code dev.dbos:transact-enterprise} runs in SQL of its
 * own. It is not application API, and it changes only as {@link EnterpriseEngine} does.
 *
 * <p>It hands out the means to run SQL, not an abstraction over the tables: that SQL addresses the
 * system database's tables directly and follows DBOS Transact's migrations.
 */
public interface EnterpriseSystemDatabase {

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
   * Whether the system database is CockroachDB, which has no advisory locks: work that would take
   * one to keep its processes from doing the same thing at once must run without it there.
   */
  boolean isCockroach();

  /**
   * Locks the workflow's status row for the rest of {@code connection}'s transaction, and fails
   * unless the execution holding {@code ownerXid} still owns the workflow. Work that changes the
   * workflow in the same transaction then cannot race a hand-off to another execution.
   *
   * <p>The lock is {@code FOR NO KEY UPDATE}, which waits for other writers of the row but not for
   * a send: a send's notification insert takes only the foreign key's {@code FOR KEY SHARE} on the
   * row. Work that must exclude a concurrent send, such as suspending a recv, locks the row {@code
   * FOR UPDATE} first.
   *
   * @param connection a connection in a transaction
   * @param workflowId the workflow
   * @param ownerXid the token of the execution that must still own the workflow
   * @throws SQLException if the database fails
   * @throws dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException if it does not
   */
  void checkOwner(Connection connection, String workflowId, String ownerXid) throws SQLException;

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
   * Decodes a value as the system database stores it: a payload column and the serialization format
   * stored beside it. A built-in format decodes with DBOS Transact's own serializer for it; any
   * other with the serializer DBOS is configured with.
   *
   * @param value the stored value, or {@code null}
   * @param serialization the stored format, or {@code null} for the configured serializer's
   * @return the decoded value, or {@code null} if {@code value} is
   * @throws IllegalArgumentException if no serializer here reads {@code serialization}
   */
  @Nullable Object deserialize(@Nullable String value, @Nullable String serialization);

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
