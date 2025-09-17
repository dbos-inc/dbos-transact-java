package dev.dbos.transact.utils;

import dev.dbos.transact.config.DBOSConfig;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBUtils {

  public static Logger logger = LoggerFactory.getLogger(DBUtils.class);

  public static void clearTables(DataSource ds) throws SQLException {

    try (Connection connection = ds.getConnection()) {
      deleteOperations(connection);
      deleteWorkflowsTestHelper(connection);
    } catch (Exception e) {
      logger.info("Error clearing tables" + e.getMessage());
      throw e;
    }
  }

  public static void deleteWorkflowsTestHelper(Connection connection) throws SQLException {

    String sql = "delete from dbos.workflow_status";

    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

      int rowsAffected = pstmt.executeUpdate();
      logger.info("Cleaned up: Deleted " + rowsAffected + " rows from dbos.workflow_status");

    } catch (SQLException e) {
      logger.error("Error deleting workflows in test helper", e);
      throw e;
    }
  }

  public static void deleteOperations(Connection connection) throws SQLException {

    String sql = "delete from dbos.operation_outputs;";

    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

      int rowsAffected = pstmt.executeUpdate();
      logger.info("Cleaned up: Deleted " + rowsAffected + " rows from dbos.operation_outputs");

    } catch (SQLException e) {
      logger.error("Error deleting workflows in test helper", e);
      throw e;
    }
  }

  public static void updateWorkflowState(DataSource ds, String oldState, String newState)
      throws SQLException {

    String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? where status = ? ;";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, newState);
      pstmt.setLong(2, Instant.now().toEpochMilli());
      pstmt.setString(3, oldState);

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();
    }
  }

  public static boolean queueEntriesAreCleanedUp(DataSource ds) throws SQLException {
    String sql = "SELECT count(*) FROM dbos.workflow_status WHERE queue_name IS NOT NULL AND status IN ('ENQUEUED', 'PENDING');";

    for (int i = 0; i < 10; i++) {
      try (Connection connection = ds.getConnection();
          Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(sql)) {
        if (rs.next()) {
          int count = rs.getInt(1);
          if (count == 0) {
            return true;
          }
        }
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }

    return false;
  }

  public void closeDS(HikariDataSource ds) {
    ds.close();
  }

  public static void recreateDB(DBOSConfig dbosConfig) throws SQLException {

    String dbUrl = String.format("jdbc:postgresql://%s:%d/%s", "localhost", 5432, "postgres");

    String sysDb = dbosConfig.getSysDbName();
    try (Connection conn = DriverManager.getConnection(dbUrl, dbosConfig.getDbUser(), dbosConfig.getDbPassword());
        Statement stmt = conn.createStatement()) {

      String dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", sysDb);
      String createDbSql = String.format("CREATE DATABASE %s", sysDb);
      stmt.execute(dropDbSql);
      stmt.execute(createDbSql);
    }
  }

  public static Connection getConnection(DBOSConfig dbosConfig) throws SQLException {
    String dbUrl = String.format("jdbc:postgresql://%s:%d/%s", "localhost", 5432, dbosConfig.getSysDbName());
    return DriverManager.getConnection(dbUrl, dbosConfig.getDbUser(), dbosConfig.getDbPassword());
  }

  public static List<Map<String, Object>> dumpWfStatus(DataSource ds) {
    try (var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT * FROM dbos.workflow_status ORDER BY \"created_at\"")) {
      var meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      List<Map<String, Object>> rows = new ArrayList<>();

      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= columnCount; i++) {
          row.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        rows.add(row);
      }

      return rows;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
