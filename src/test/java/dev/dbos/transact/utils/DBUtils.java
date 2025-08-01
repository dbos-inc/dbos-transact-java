package dev.dbos.transact.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.workflow.WorkflowState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DBUtils {

    public static Logger logger = LoggerFactory.getLogger(DBUtils.class) ;

    public static void clearTables(DataSource ds) throws SQLException {

        try (Connection connection = ds.getConnection()) {
            deleteOperations(connection);
            deleteWorkflowsTestHelper(connection); ;
        } catch(Exception e) {
            logger.info("Error clearing tables" + e.getMessage()) ;
            throw e ;
        }

    }

    public static void deleteWorkflowsTestHelper(Connection connection) throws SQLException {

        String sql = "delete from dbos.workflow_status";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            int rowsAffected = pstmt.executeUpdate();
            logger.info("Cleaned up: Deleted " + rowsAffected + " rows from dbos.workflow_status");

        } catch (SQLException e) {
            logger.error("Error deleting workflows in test helper: " + e.getMessage());
            throw e;
        }

    }

    public static void deleteOperations(Connection connection) throws SQLException{

        String sql = "delete from dbos.operation_outputs;";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            int rowsAffected = pstmt.executeUpdate();
            logger.info("Cleaned up: Deleted " + rowsAffected + " rows from dbos.operation_outputs");

        } catch (SQLException e) {
            logger.error("Error deleting workflows in test helper: " + e.getMessage());
            throw e;
        }

    }

    public static void updateWorkflowState(DataSource ds, String oldState, String newState) throws SQLException {

        String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? where status = ? ;";

        try (Connection connection = ds.getConnection();
             PreparedStatement pstmt = connection.prepareStatement(sql)) {

            pstmt.setString(1, newState);
            pstmt.setLong(2, Instant.now().toEpochMilli());
            pstmt.setString(3, oldState) ;

            // Execute the update and get the number of rows affected
            int rowsAffected = pstmt.executeUpdate();


        }
    }

    public void closeDS(HikariDataSource ds) {
        ds.close();
    }

    public static void recreateDB(DBOSConfig dbosConfig) throws SQLException{

        String dbUrl = String.format("jdbc:postgresql://%s:%d/%s", "localhost", 5432, "postgres");

        String sysDb = dbosConfig.getSysDbName();
        try (Connection conn = DriverManager.getConnection(dbUrl, dbosConfig.getDbUser(), dbosConfig.getDbPassword());
             Statement stmt = conn.createStatement()) {


            String dropDbSql = String.format("DROP DATABASE IF EXISTS %s", sysDb);
            String createDbSql = String.format("CREATE DATABASE %s", sysDb);
            stmt.execute(dropDbSql);
            stmt.execute(createDbSql);
        }
    }
}
