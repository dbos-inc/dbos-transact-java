package dev.dbos.transact.cli;

import dev.dbos.transact.migrations.MigrationManager;

import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtils {
  public static void dropDatabase(String url, String user, String password) throws SQLException {
    var pair = MigrationManager.extractDbAndPostgresUrl(url);
    var dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", pair.database());
    try (var conn = DriverManager.getConnection(pair.url(), user, password);
        var stmt = conn.createStatement()) {
      stmt.execute(dropDbSql);
    }
  }

  static boolean checkConnection(String url, String user, String password) {
    try (var conn = DriverManager.getConnection(url, user, password);
        var stmt = conn.createStatement()) {
      stmt.execute("SELECT 1");
      return true;
    } catch (SQLException e) {
      return false;
    }
  }
}
