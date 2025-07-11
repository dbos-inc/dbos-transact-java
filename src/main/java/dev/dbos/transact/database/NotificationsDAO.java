package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.internal.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NotificationsDAO {

    Logger logger = LoggerFactory.getLogger(QueuesDAO.class);
    private DataSource dataSource ;
    private StepsDAO stepsDAO ;

    NotificationsDAO(DataSource ds, StepsDAO stepsDAO) {
        this.dataSource = ds ;
        this.stepsDAO = stepsDAO;
    }

    public void send(String workflowUuid, int functionId, String destinationUuid,
                     Object message, String topic) throws SQLException {

        String functionName = "DBOS.send";
        String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // Check if operation was already executed
                StepResult recordedOutput = stepsDAO.checkStepExecutionTxn(workflowUuid, functionId, functionName, conn);

                if (recordedOutput != null) {
                    logger.debug(String.format("Replaying send, id: %d, destination_uuid: %s, topic: %s",
                            functionId, destinationUuid, finalTopic));
                    conn.commit();
                    return;
                } else {
                    logger.debug(String.format("Running send, id: %d, destination_uuid: %s, topic: %s",
                            functionId, destinationUuid, finalTopic));
                }

                // Insert notification
                String insertSql = "INSERT INTO %.notifications (destination_uuid, topic, message) " +
                                " VALUES (?, ?, ?) " ;

                insertSql = String.format(insertSql, Constants.DB_SCHEMA) ;

                try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
                    stmt.setString(1, destinationUuid);
                    stmt.setString(2, finalTopic);
                    stmt.setString(3, JSONUtil.serialize(message));
                    stmt.executeUpdate();
                } catch (SQLException e) {
                    // Foreign key violation
                    if ("23503".equals(e.getSQLState())) {
                        throw new NonExistentWorkflowException(destinationUuid);
                    }
                    throw e;
                }

                // Record operation result
                StepResult output = new StepResult();
                output.setWorkflowId(workflowUuid);
                output.setFunctionId(functionId);
                output.setFunctionName(functionName);
                output.setOutput(null);
                output.setError(null);

                stepsDAO.recordStepResultTxn(output, conn);

                conn.commit();

            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                throw e;
            }
        }
    }
}
