package dev.dbos.transact.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.MigrationManager;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBUtils {

  private static final Logger logger = LoggerFactory.getLogger(DBUtils.class);

  public static void clearTables(DataSource ds) throws SQLException {

    try (Connection connection = ds.getConnection()) {
      deleteAllOperationOutputs(connection);
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

  public static void deleteAllOperationOutputs(Connection connection) throws SQLException {

    String sql = "delete from dbos.operation_outputs;";

    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

      int rowsAffected = pstmt.executeUpdate();
      logger.info("Cleaned up: Deleted " + rowsAffected + " rows from dbos.operation_outputs");

    } catch (SQLException e) {
      logger.error("Error deleting workflows in test helper", e);
      throw e;
    }
  }

  public static int updateAllWorkflowStates(DataSource ds, String oldState, String newState)
      throws SQLException {

    String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? where status = ? ;";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, newState);
      pstmt.setLong(2, Instant.now().toEpochMilli());
      pstmt.setString(3, oldState);

      // Execute the update and get the number of rows affected
      return pstmt.executeUpdate();
    }
  }

  public static void setWorkflowState(DataSource ds, String workflowId, String newState)
      throws SQLException {

    String sql =
        "UPDATE dbos.workflow_status SET status = ?, updated_at = ? WHERE workflow_uuid = ?";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, newState);
      pstmt.setLong(2, Instant.now().toEpochMilli());
      pstmt.setString(3, workflowId);

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();

      assertEquals(1, rowsAffected);
    }
  }

  public static void deleteStepOutput(DataSource ds, String workflowId, int function_id)
      throws SQLException {

    String sql = "DELETE from dbos.operation_outputs WHERE workflow_uuid = ? and function_id = ?;";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, workflowId);
      pstmt.setInt(2, function_id);

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();

      assertEquals(1, rowsAffected);
    }
  }

  public static void deleteAllStepOutputs(DataSource ds, String workflowId) throws SQLException {

    String sql = "DELETE from dbos.operation_outputs WHERE workflow_uuid = ?";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, workflowId);

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();

      assertEquals(2, rowsAffected);
    }
  }

  public static void updateStepEndTime(
      DataSource ds, String workflowId, int functionId, String endtime) throws SQLException {

    String sql =
        "update dbos.operation_outputs SET output = ? WHERE workflow_uuid = ? AND function_id = ? ";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, endtime);
      pstmt.setString(2, workflowId);
      pstmt.setInt(3, functionId);

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();

      assertEquals(1, rowsAffected);
    }
  }

  public static boolean queueEntriesAreCleanedUp(DataSource ds) throws SQLException {
    String sql =
        "SELECT count(*) FROM dbos.workflow_status WHERE queue_name IS NOT NULL AND status IN ('ENQUEUED', 'PENDING');";

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

  public static void recreateDB(DBOSConfig config) throws SQLException {
    recreateDB(config.databaseUrl(), config.dbUser(), config.dbPassword());
  }

  public static void recreateDB(String url, String user, String password) throws SQLException {
    var pair = MigrationManager.extractDbAndPostgresUrl(url);
    var dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", pair.database());
    var createDbSql = String.format("CREATE DATABASE %s", pair.database());
    try (var conn = DriverManager.getConnection(pair.url(), user, password);
        var stmt = conn.createStatement()) {
      stmt.execute(dropDbSql);
      stmt.execute(createDbSql);
    }
  }

  public static Connection getConnection(DBOSConfig config) throws SQLException {
    return DriverManager.getConnection(config.databaseUrl(), config.dbUser(), config.dbPassword());
  }

  public static List<WorkflowStatusRow> getWorkflowRows(DataSource ds) {
    try (var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT * FROM dbos.workflow_status ORDER BY \"created_at\"")) {
      List<WorkflowStatusRow> rows = new ArrayList<>();

      while (rs.next()) {
        rows.add(new WorkflowStatusRow(rs));
      }

      return rows;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static WorkflowStatusRow getWorkflowRow(DataSource ds, String workflowId) {
    var sql = "SELECT * FROM dbos.workflow_status WHERE workflow_uuid = ?";

    try (var conn = ds.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);

      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return new WorkflowStatusRow(rs);
        } else {
          return null;
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<OperationOutputRow> getStepRows(DataSource ds, String workflowId) {
    var sql = "SELECT * FROM dbos.operation_outputs WHERE workflow_uuid = ? ORDER BY function_id";
    try (var conn = ds.getConnection();
        var stmt = conn.prepareStatement(sql)) {

      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        List<OperationOutputRow> rows = new ArrayList<>();

        while (rs.next()) {
          rows.add(new OperationOutputRow(rs));
        }
        return rows;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<String> getWorkflowEvents(DataSource ds, String workflowId) {
    try (var conn = ds.getConnection(); ) {
      var stmt =
          conn.prepareStatement("SELECT * FROM dbos.workflow_events WHERE workflow_uuid = ?");
      stmt.setString(1, workflowId);
      var rs = stmt.executeQuery();
      List<String> rows = new ArrayList<>();

      while (rs.next()) {
        rows.add(rs.getString("key") + "=" + rs.getString("value"));
      }

      return rows;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static void causeChaos(DataSource ds) {
    try (Connection conn = ds.getConnection();
        Statement st = conn.createStatement()) {

      st.execute(
          """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND datname = current_database();
        """);
    } catch (SQLException e) {
      throw new RuntimeException("Could not cause chaos, credentials insufficient?", e);
    }
  }
}
