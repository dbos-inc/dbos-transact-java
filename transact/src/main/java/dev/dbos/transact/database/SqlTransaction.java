package dev.dbos.transact.database;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Runs a unit of work inside an explicit JDBC transaction on a connection that is otherwise in
 * autocommit mode, restoring autocommit afterwards.
 *
 * <p>Rollback covers every {@link Throwable}, not just {@link SQLException}. An unchecked exception
 * escaping the action would otherwise reach the {@code finally} block, where restoring autocommit
 * commits the partial transaction — the opposite of what the caller intended.
 */
public final class SqlTransaction {

  /** A unit of work to run against a connection already placed in an explicit transaction. */
  @FunctionalInterface
  public interface SqlAction {
    void run(Connection conn) throws SQLException;
  }

  /** A unit of work that produces a value. */
  @FunctionalInterface
  public interface SqlCall<T> {
    T run(Connection conn) throws SQLException;
  }

  private SqlTransaction() {}

  /**
   * Runs {@code action} in a transaction, committing on success and rolling back on any failure.
   *
   * @param conn a connection in autocommit mode; left in autocommit mode on return
   * @param action the work to perform
   * @throws SQLException if the action or the transaction itself fails
   */
  public static void run(Connection conn, SqlAction action) throws SQLException {
    call(
        conn,
        c -> {
          action.run(c);
          return null;
        });
  }

  /**
   * Runs {@code action} in a transaction and returns its result, committing on success and rolling
   * back on any failure.
   *
   * @param conn a connection in autocommit mode; left in autocommit mode on return
   * @param action the work to perform
   * @return whatever the action returned
   * @throws SQLException if the action or the transaction itself fails
   */
  public static <T> T call(Connection conn, SqlCall<T> action) throws SQLException {
    conn.setAutoCommit(false);
    T result;
    try {
      result = action.run(conn);
      conn.commit();
    } catch (Throwable t) {
      // Restoring autocommit can itself fail on a broken connection. Suppress both cleanup
      // failures onto the original throwable rather than letting a finally block replace it.
      try {
        conn.rollback();
      } catch (SQLException rollbackFailure) {
        t.addSuppressed(rollbackFailure);
      }
      try {
        conn.setAutoCommit(true);
      } catch (SQLException restoreFailure) {
        t.addSuppressed(restoreFailure);
      }
      throw t;
    }
    conn.setAutoCommit(true);
    return result;
  }
}
