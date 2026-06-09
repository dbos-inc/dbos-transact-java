package dev.dbos.transact.txstep;

import java.sql.Connection;

/**
 * Transaction isolation levels supported by PostgreSQL.
 *
 * <p>Pass as part of {@link StepFactoryOptions} to request a specific isolation level for a
 * transactional step. {@link #DEFAULT} leaves the connection's isolation level unchanged.
 */
public enum IsolationLevel {
  /** Do not override the connection's isolation level (datasource/pool default). */
  DEFAULT(-1),
  READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
  READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
  REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
  SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

  private final int jdbcValue;

  IsolationLevel(int jdbcValue) {
    this.jdbcValue = jdbcValue;
  }

  /** The JDBC constant for this level, or {@code -1} for {@link #DEFAULT}. */
  public int jdbcValue() {
    return jdbcValue;
  }
}
