package dev.dbos.transact.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.config.DBOSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBUtils {

    public static Logger logger = LoggerFactory.getLogger(DBUtils.class) ;

    public static DataSource createDataSource(DBOSConfig config) {

        HikariConfig hikariConfig = new HikariConfig();

        String dburl = String.format("jdbc:postgresql://%s:%d/%s",config.getDbHost(),config.getDbPort(), config.getSysDbName());

        hikariConfig.setJdbcUrl(dburl);
        hikariConfig.setUsername(config.getDbUser());
        hikariConfig.setPassword(config.getDbPassword());

        int maximumPoolSize = config.getMaximumPoolSize();
        if (maximumPoolSize > 0) {
            hikariConfig.setMaximumPoolSize(maximumPoolSize);
        } else {
            hikariConfig.setMaximumPoolSize(5);
        }

        int connectionTimeout = config.getConnectionTimeout();
        if (connectionTimeout > 0) {
            hikariConfig.setConnectionTimeout(connectionTimeout);
        }

       return new HikariDataSource(hikariConfig);
    }

    public static void clearTables(DataSource ds) throws SQLException {

        try (Connection connection = ds.getConnection()) {
            deleteOperations(connection);
            deleteWorkflowsTestHelper(connection); ;
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

    public void closeDS(HikariDataSource ds) {
        ds.close();
    }
}
