package dev.dbos.transact.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import org.jspecify.annotations.Nullable;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A try of the lock letting one of an application's processes run a background job at a time. Held,
 * when acquired, by an otherwise idle transaction on one pooled connection until closed.
 */
public final class BackgroundJobLock implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(BackgroundJobLock.class);

  // How long the lock's transaction may sit idle before Postgres ends its holder's session.
  public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(5);

  private final String jobName;
  private final @Nullable Connection conn;
  private final boolean acquired;
  private final String renewSql;

  private BackgroundJobLock(
      String jobName, @Nullable Connection conn, boolean acquired, String renewSql) {
    this.jobName = jobName;
    this.conn = conn;
    this.acquired = acquired;
    this.renewSql = renewSql;
  }

  static BackgroundJobLock tryAcquire(DbContext ctx, String jobName, Duration idleTimeout)
      throws SQLException {
    // CockroachDB grants every advisory lock, so each process runs every job.
    if (ctx.isCockroach()) {
      return new BackgroundJobLock(jobName, null, true, "");
    }
    long key = AdvisoryLocks.backgroundJobKey(ctx.schema(), ctx.appName(), jobName);
    long timeoutMs = Math.max(1, idleTimeout.toMillis());
    var conn = ctx.getConnection();
    try {
      conn.setAutoCommit(false);
      // The local idle timeout ends a frozen holder's session, releasing its lock. Only integers
      // are formatted into the text.
      var value =
          firstValueOfLast(
              conn,
              "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; "
                  + "SELECT pg_try_advisory_xact_lock(%d) FROM set_config(".formatted(key)
                  + "'idle_in_transaction_session_timeout', '%d', true)".formatted(timeoutMs));
      var acquired = value instanceof Boolean b ? b : "t".equals(String.valueOf(value));
      return new BackgroundJobLock(
          jobName,
          conn,
          acquired,
          "SET LOCAL idle_in_transaction_session_timeout = '%d'".formatted(timeoutMs));
    } catch (Throwable e) {
      // An Error too, or the connection would never return to the pool.
      release(conn, jobName);
      throw e;
    }
  }

  public boolean acquired() {
    return acquired;
  }

  // Resets the idle timeout in place, so the lock is never free mid-hold. Snapshot-free, so a
  // concurrent index build never waits on it; fails if Postgres has ended the session.
  public void renew() throws SQLException {
    if (conn == null || !acquired) {
      return;
    }
    firstValueOfLast(conn, renewSql);
  }

  @Override
  public void close() {
    if (conn != null) {
      release(conn, jobName);
    }
  }

  private static void release(Connection conn, String jobName) {
    try {
      conn.rollback();
    } catch (SQLException e) {
      // A session the idle timeout ended has released the lock already.
      logger.debug("Could not release the {} lock: {}", jobName, e.getMessage());
    } finally {
      try {
        conn.close();
      } catch (SQLException e) {
        logger.debug("Could not close the {} lock's connection: {}", jobName, e.getMessage());
      }
    }
  }

  // Sent through the simple query protocol where the driver allows it: a bound or prepared
  // statement's portal keeps its snapshot, and so an xmin, while the session idles.
  private static @Nullable Object firstValueOfLast(Connection conn, String sql)
      throws SQLException {
    try (var stmt = conn.createStatement()) {
      var isResultSet = execute(stmt, sql);
      Object value = null;
      while (true) {
        if (isResultSet) {
          try (var rs = stmt.getResultSet()) {
            value = rs.next() ? rs.getObject(1) : null;
          }
        } else if (stmt.getUpdateCount() == -1) {
          return value;
        } else {
          value = null;
        }
        isResultSet = stmt.getMoreResults();
      }
    }
  }

  private static boolean execute(Statement stmt, String sql) throws SQLException {
    if (stmt.isWrapperFor(BaseStatement.class)) {
      return stmt.unwrap(BaseStatement.class)
          .executeWithFlags(sql, QueryExecutor.QUERY_EXECUTE_AS_SIMPLE);
    }
    return stmt.execute(sql);
  }
}
