package dev.dbos.transact.database;

import dev.dbos.transact.json.DBOSSerializer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.BooleanSupplier;

import javax.sql.DataSource;

public record DbContext(
    DataSource dataSource,
    String schema,
    DBOSSerializer serializer,
    BooleanSupplier closed,
    String executorId,
    PollingLimiter pollingLimiter) {

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  /**
   * Acquire a permit for one polling read, to be held across the connection it checks out. Only the
   * wait loops take one; see {@link PollingLimiter}.
   */
  public PollingLimiter.Permit acquirePollPermit() {
    return pollingLimiter.acquire();
  }

  public void checkClosed() {
    if (closed.getAsBoolean()) {
      throw new IllegalStateException("Database is closed");
    }
  }
}
