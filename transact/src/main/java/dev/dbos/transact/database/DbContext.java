package dev.dbos.transact.database;

import dev.dbos.transact.json.DBOSSerializer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.BooleanSupplier;

import javax.sql.DataSource;

record DbContext(
    DataSource dataSource, String schema, DBOSSerializer serializer, BooleanSupplier closed) {

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public void checkClosed() {
    if (closed.getAsBoolean()) {
      throw new IllegalStateException("Database is closed");
    }
  }
}
