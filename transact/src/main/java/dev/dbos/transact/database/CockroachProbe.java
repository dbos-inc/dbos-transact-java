package dev.dbos.transact.database;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;

/**
 * Whether a system database is CockroachDB, asked of the database once and remembered.
 *
 * <p>A probe that fails is not remembered, so a database unreachable at startup is asked again on
 * the next use rather than guessed at for the life of the process.
 */
public final class CockroachProbe {

  private volatile @Nullable Boolean cockroach;

  /** A probe that asks the database on first use. */
  public CockroachProbe() {}

  /** Whether {@code dataSource} is CockroachDB, checking out a connection only the first time. */
  public boolean isCockroach(DataSource dataSource) throws SQLException {
    var known = cockroach;
    if (known == null) {
      known = SystemDatabase.isCockroach(dataSource);
      cockroach = known;
    }
    return known;
  }

  /**
   * Whether the database {@code connection} is on is CockroachDB, asking on it only the first time.
   * Call it before the connection's transaction begins: a connection outside autocommit is rolled
   * back after asking, so its transaction can still be given an isolation level.
   */
  public boolean isCockroach(Connection connection) throws SQLException {
    var known = cockroach;
    if (known == null) {
      known = SystemDatabase.isCockroach(connection);
      if (!connection.getAutoCommit()) {
        connection.rollback();
      }
      cockroach = known;
    }
    return known;
  }
}
