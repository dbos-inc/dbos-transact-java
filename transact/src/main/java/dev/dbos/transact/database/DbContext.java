package dev.dbos.transact.database;

import dev.dbos.transact.json.DBOSSerializer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.BooleanSupplier;

import javax.sql.DataSource;

record DbContext(
    DataSource dataSource, String schema, DBOSSerializer serializer, BooleanSupplier closed) {

  Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  boolean isClosed() {
    return closed.getAsBoolean();
  }
}
