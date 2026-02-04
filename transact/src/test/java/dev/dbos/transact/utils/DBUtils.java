package dev.dbos.transact.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

  public static List<WorkflowStatusRow> getWorkflowRows(DataSource ds) throws SQLException {
    return getWorkflowRows(ds, null);
  }

  public static List<WorkflowStatusRow> getWorkflowRows(DataSource ds, String schema)
      throws SQLException {
    schema = SystemDatabase.sanitizeSchema(schema);
    String sql = "SELECT * FROM %s.workflow_status ORDER BY created_at".formatted(schema);
    try (var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      List<WorkflowStatusRow> rows = new ArrayList<>();
      while (rs.next()) {
        rows.add(new WorkflowStatusRow(rs));
      }
      return rows;
    }
  }

  public static WorkflowStatusRow getWorkflowRow(DataSource ds, String workflowId)
      throws SQLException {
    return getWorkflowRow(ds, workflowId, null);
  }

  public static WorkflowStatusRow getWorkflowRow(DataSource ds, String workflowId, String schema)
      throws SQLException {
    schema = SystemDatabase.sanitizeSchema(schema);
    var sql = "SELECT * FROM %s.workflow_status WHERE workflow_uuid = ?".formatted(schema);
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
    }
  }

  public static List<OperationOutputRow> getStepRows(DataSource ds, String workflowId)
      throws SQLException {
    return getStepRows(ds, workflowId, null);
  }

  public static List<OperationOutputRow> getStepRows(
      DataSource ds, String workflowId, String schema) throws SQLException {
    schema = SystemDatabase.sanitizeSchema(schema);
    var sql =
        "SELECT * FROM %s.operation_outputs WHERE workflow_uuid = ? ORDER BY function_id"
            .formatted(schema);
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
    }
  }

  public record Event(String key, String value, String serialization) {}

  public static List<Event> getWorkflowEvents(DataSource ds, String workflowId)
      throws SQLException {
    return getWorkflowEvents(ds, workflowId, null);
  }

  public static List<Event> getWorkflowEvents(DataSource ds, String workflowId, String schema)
      throws SQLException {
    schema = SystemDatabase.sanitizeSchema(schema);
    try (var conn = ds.getConnection(); ) {
      var stmt =
          conn.prepareStatement(
              "SELECT * FROM %s.workflow_events WHERE workflow_uuid = ?".formatted(schema));
      stmt.setString(1, workflowId);
      var rs = stmt.executeQuery();
      List<Event> rows = new ArrayList<>();

      while (rs.next()) {
        var key = rs.getString("key");
        var value = rs.getString("value");
        var serialization = rs.getString("serialization");
        rows.add(new Event(key, value, serialization));
      }

      return rows;
    }
  }

  public record EventHistory(int stepId, String key, String value, String serialization) {}

  public static List<EventHistory> getWorkflowEventHistory(DataSource ds, String workflowId)
      throws SQLException {
    return getWorkflowEventHistory(ds, workflowId, null);
  }

  public static List<EventHistory> getWorkflowEventHistory(
      DataSource ds, String workflowId, String schema) throws SQLException {
    schema = SystemDatabase.sanitizeSchema(schema);
    try (var conn = ds.getConnection(); ) {
      var stmt =
          conn.prepareStatement(
              "SELECT * FROM %s.workflow_events_history WHERE workflow_uuid = ?".formatted(schema));
      stmt.setString(1, workflowId);
      var rs = stmt.executeQuery();
      List<EventHistory> rows = new ArrayList<>();

      while (rs.next()) {
        var stepId = rs.getInt("function_id");
        var key = rs.getString("key");
        var value = rs.getString("value");
        var serialization = rs.getString("serialization");
        rows.add(new EventHistory(stepId, key, value, serialization));
      }

      return rows;
    }
  }

  public record Notification(
      String destinationUuid, String topic, String message, String serialization) {}

  public static List<Notification> getNotifications(DataSource ds, String destinationUuid)
      throws SQLException {
    return getNotifications(ds, destinationUuid, null);
  }

  public static List<Notification> getNotifications(
      DataSource ds, String destinationUuid, String schema) throws SQLException {
    schema = SystemDatabase.sanitizeSchema(schema);
    var sql =
        "SELECT * FROM %s.notifications WHERE destination_uuid = ? ORDER BY created_at_epoch_ms"
            .formatted(schema);
    try (var conn = ds.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, destinationUuid);
      try (var rs = stmt.executeQuery()) {
        List<Notification> rows = new ArrayList<>();
        while (rs.next()) {
          var topic = rs.getString("topic");
          var message = rs.getString("message");
          var serialization = rs.getString("serialization");
          rows.add(new Notification(destinationUuid, topic, message, serialization));
        }
        return rows;
      }
    }
  }

  public static boolean queueEntriesCleanedUp(DataSource ds) throws SQLException {
    return queueEntriesCleanedUp(ds, null);
  }

  public static boolean queueEntriesCleanedUp(DataSource ds, String schema) throws SQLException {
    schema = SystemDatabase.sanitizeSchema(schema);
    var sql =
        """
      SELECT COUNT(*) FROM %s.workflow_status
      WHERE queue_name IS NOT NULL
        AND queue_name != ?
        AND status IN ('ENQUEUED', 'PENDING')
      """
            .formatted(schema);

    boolean success = false;
    for (var i = 0; i < 10; i++) {
      try (var conn = ds.getConnection();
          var ps = conn.prepareStatement(sql)) {
        ps.setString(1, Constants.DBOS_INTERNAL_QUEUE);
        try (var rs = ps.executeQuery()) {
          if (!rs.next()) {
            continue;
          }
          var count = rs.getInt(1);
          if (count == 0) {
            success = true;
            break;
          }
        }
      }
    }
    return success;
  }
}
