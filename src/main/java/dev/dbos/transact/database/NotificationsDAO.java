package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.DBOSWorkflowConflictException;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.notifications.NotificationService;
import dev.dbos.transact.workflow.internal.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class NotificationsDAO {

    Logger logger = LoggerFactory.getLogger(QueuesDAO.class);
    private DataSource dataSource ;
    private StepsDAO stepsDAO ;
    private NotificationService notificationService;

    NotificationsDAO(DataSource ds, StepsDAO stepsDAO, NotificationService nService) {
        this.dataSource = ds ;
        this.stepsDAO = stepsDAO;
        this.notificationService = nService;
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
                String insertSql = "INSERT INTO %s.notifications (destination_uuid, topic, message) " +
                                " VALUES (?, ?, ?) " ;


                insertSql = String.format(insertSql, Constants.DB_SCHEMA) ;

                logger.info(insertSql) ;

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

    public Object recv(String workflowUuid, int functionId, int timeoutFunctionId,
                       String topic, double timeoutSeconds) throws SQLException, InterruptedException {

        String functionName = "DBOS.recv";
        String finalTopic = (topic != null) ? topic : Constants.DBOS_NULL_TOPIC ;

        // First, check for previous executions
        StepResult recordedOutput = null ;

        try (Connection c = dataSource.getConnection()) {
            logger.info("mjjjj checking recorded output for recv" + workflowUuid + " " + functionId) ;
            recordedOutput = stepsDAO.checkStepExecutionTxn(workflowUuid, functionId, functionName, c);
        }

        if (recordedOutput != null) {
            logger.debug(String.format("Replaying recv, id: %d, topic: %s", functionId, finalTopic));
            if (recordedOutput.getOutput() != null) {
                // return JSONUtil.deserializeToArray(recordedOutput.getOutput());
                Object[] dSerOut = JSONUtil.deserializeToArray(recordedOutput.getOutput());
                if (dSerOut !=  null) {
                    logger.info("mjjjjj returning output" + dSerOut.toString()) ;
                }
                return dSerOut == null ? null : dSerOut[0];

            } else {
                throw new RuntimeException("No output recorded in the last recv");
            }
        } else {
            logger.debug(String.format("Running recv, id: %d, topic: %s", functionId, finalTopic));
        }

        // Insert a condition to the notifications map
        String payload = workflowUuid + "::" + finalTopic;
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        try {
            lock.lock();
            boolean success = notificationService.registerNotificationCondition(payload, new NotificationService.LockConditionPair());
            if (!success) {
                // This should not happen, but if it does, it means the workflow is executed concurrently
                throw new DBOSWorkflowConflictException(workflowUuid, "Workflow might be executing concurrently. ");
            }

            // Check if the key is already in the database. If not, wait for the notification
            boolean hasExistingNotification = false;
            try (Connection conn = dataSource.getConnection()) {
                String checkSql = " SELECT topic FROM %s.notifications " +
                                    " WHERE destination_uuid = ? AND topic = ? " ;

                checkSql = String.format(checkSql, Constants.DB_SCHEMA);

                try (PreparedStatement stmt = conn.prepareStatement(checkSql)) {
                    stmt.setString(1, workflowUuid);
                    stmt.setString(2, finalTopic);
                    try (ResultSet rs = stmt.executeQuery()) {
                        hasExistingNotification = rs.next();
                    }
                }
            }

            if (!hasExistingNotification) {
                // Wait for the notification
                // Support OAOO sleep
                double actualTimeout = sleep(workflowUuid, timeoutFunctionId, timeoutSeconds, true);
                // double actualTimeout = 5 ;
                long timeoutMs = (long) (actualTimeout * 1000);
                condition.await(timeoutMs, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
            notificationService.unregisterNotificationCondition(payload);
        }

        // Transactionally consume and return the message if it's in the database
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // Find and delete the oldest entry for this workflow+topic
                String deleteAndReturnSql = " WITH oldest_entry AS ( " +
                    " SELECT destination_uuid, topic, message, created_at_epoch_ms " +
                    " FROM %s.notifications " +
                    " WHERE destination_uuid = ? AND topic = ? " +
                    " ORDER BY created_at_epoch_ms ASC " +
                    " LIMIT 1 " +
                " ) " +
                " DELETE FROM %s.notifications " +
                " WHERE destination_uuid = (SELECT destination_uuid FROM oldest_entry) " +
                "  AND topic = (SELECT topic FROM oldest_entry) " +
                " AND created_at_epoch_ms = (SELECT created_at_epoch_ms FROM oldest_entry) " +
                " RETURNING message " ;

                deleteAndReturnSql = String.format(deleteAndReturnSql, Constants.DB_SCHEMA, Constants.DB_SCHEMA);

                Object[] recvdSermessage = null;
                try (PreparedStatement stmt = conn.prepareStatement(deleteAndReturnSql)) {
                    stmt.setString(1, workflowUuid);
                    stmt.setString(2, finalTopic);

                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            String serializedMessage = rs.getString("message");
                            recvdSermessage = JSONUtil.deserializeToArray(serializedMessage);
                        }
                    }
                }

                // Record operation result
                StepResult output = new StepResult();
                output.setWorkflowId(workflowUuid);
                output.setFunctionId(functionId);
                output.setFunctionName(functionName);
                Object toSave =  recvdSermessage == null ? null : recvdSermessage[0] ;
                output.setOutput(JSONUtil.serialize(toSave));
                output.setError(null);

                stepsDAO.recordStepResultTxn(output, conn);

                conn.commit();

                // return dSermessage != null ? dSermessage[0] : null;
                return toSave;

            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    // TODO : can be moved elsewhere when we implement DBOS.sleep
    public double sleep(String workflowUuid, int functionId, double seconds, boolean skipSleep) throws SQLException {
        String functionName = "DBOS.sleep";

        StepResult recordedOutput = null ;

        try (Connection c = dataSource.getConnection()) {
            recordedOutput = stepsDAO.checkStepExecutionTxn(workflowUuid, functionId, functionName, c) ;
        }

        Double endTime;
        if (recordedOutput != null) {
            logger.debug("Replaying sleep, id: {}, seconds: {}", functionId, seconds);
            String output = recordedOutput.getOutput();
            if (output == null) {
                throw new AssertionError("no recorded end time");
            }
            endTime = (Double)JSONUtil.deserializeToArray(output)[0] ;
        } else {
            logger.debug("Running sleep, id: {}, seconds: {}", functionId, seconds);
            endTime = System.currentTimeMillis() / 1000.0 + seconds;

            try {

                StepResult output = new StepResult();
                output.setWorkflowId(workflowUuid);
                output.setFunctionId(functionId);
                output.setFunctionName(functionName);
                output.setOutput(JSONUtil.serialize(endTime));
                output.setError(null);

                try(Connection conn = dataSource.getConnection()) {
                    stepsDAO.recordStepResultTxn(output, conn);
                }
            } catch (DBOSWorkflowConflictException e) {
                // Ignore conflict - operation already recorded
                logger.error("Error recording sleep error ",e);
            }
        }

        double duration = Math.max(0, endTime - (System.currentTimeMillis() / 1000.0));

        if (!skipSleep) {
            try {
                Thread.sleep((long) (duration * 1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Sleep interrupted", e);
            }
        }

        return duration;
    }

}
