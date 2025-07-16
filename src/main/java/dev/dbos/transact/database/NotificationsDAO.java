package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.exceptions.DBOSWorkflowConflictException;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.notifications.GetWorkflowEventContext;
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
            recordedOutput = stepsDAO.checkStepExecutionTxn(workflowUuid, functionId, functionName, c);
        }

        if (recordedOutput != null) {
            logger.debug(String.format("Replaying recv, id: %d, topic: %s", functionId, finalTopic));
            if (recordedOutput.getOutput() != null) {
                Object[] dSerOut = JSONUtil.deserializeToArray(recordedOutput.getOutput());
                return dSerOut == null ? null : dSerOut[0];
            } else {
                throw new RuntimeException("No output recorded in the last recv");
            }
        } else {
            logger.debug(String.format("Running recv, id: %d, topic: %s", functionId, finalTopic));
        }

        // Insert a condition to the notifications map
        String payload = workflowUuid + "::" + finalTopic;
        NotificationService.LockConditionPair lockPair = new NotificationService.LockConditionPair();

        try {
            lockPair.lock.lock();
            boolean success = notificationService.registerNotificationCondition(payload, lockPair);
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
                long timeoutMs = (long) (actualTimeout * 1000);
                lockPair.condition.await(timeoutMs, TimeUnit.MILLISECONDS);

            } else {
                logger.info("We have notification. no need to sleep") ;
            }
        } finally {
            lockPair.lock.unlock();
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

    public void setEvent(String workflowId, int functionId, String key, Object message) throws SQLException {
        String functionName = "DBOS.setEvent";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // Check if operation was already executed
                StepResult recordedOutput = stepsDAO.checkStepExecutionTxn(
                        workflowId, functionId, functionName, conn
                );


                if (recordedOutput != null) {
                    logger.debug("Replaying setEvent, id: {}, key: {}", functionId, key);
                    conn.commit();
                    return; // Already sent before
                } else {
                    logger.debug("Running setEvent, id: {}, key: {}", functionId, key);
                }

                // Serialize the message
                String serializedMessage = JSONUtil.serialize(message);

                // Insert or update the workflow event using UPSERT
                String upsertSql = " INSERT INTO %s.workflow_events (workflow_uuid, key, value) " +
                                    " VALUES (?, ?, ?) " +
                                    " ON CONFLICT (workflow_uuid, key) " +
                                    " DO UPDATE SET value = EXCLUDED.value" ;

                upsertSql = String.format(upsertSql, Constants.DB_SCHEMA);

                try (PreparedStatement stmt = conn.prepareStatement(upsertSql)) {
                    stmt.setString(1, workflowId);
                    stmt.setString(2, key);
                    stmt.setString(3, serializedMessage);
                    stmt.executeUpdate();
                }

                // Create operation result
                StepResult output = new StepResult();
                output.setWorkflowId(workflowId);
                output.setFunctionId(functionId);
                output.setFunctionName(functionName);
                output.setOutput(null);
                output.setError(null);

                // Record the operation result
                stepsDAO.recordStepResultTxn(output, conn);

                conn.commit();

            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    public Object getEvent(String targetUuid, String key, double timeoutSeconds, GetWorkflowEventContext callerCtx) throws SQLException{
        String functionName = "DBOS.getEvent";

        // Check for previous executions only if it's in a workflow
        if (callerCtx != null) {

            StepResult recordedOutput = null ;

            try (Connection conn = dataSource.getConnection()) {
                recordedOutput = stepsDAO.checkStepExecutionTxn(
                    callerCtx.getWorkflowId(), callerCtx.getFunctionId(), functionName, conn
                );
            }


            if (recordedOutput != null) {
                logger.debug("Replaying getEvent, id: {}, key: {}", callerCtx.getFunctionId(), key);
                if (recordedOutput.getOutput() != null) {
                    // return deserialize(recordedOutput.getOutput());
                    Object[] outputArray = JSONUtil.deserializeToArray(recordedOutput.getOutput());
                    return outputArray == null ? null : outputArray[0];
                } else {
                    throw new RuntimeException("No output recorded in the last getEvent");
                }
            } else {
                logger.debug("Running getEvent, id: {}, key: {}", callerCtx.getFunctionId(), key);
            }
        }

        String payload = targetUuid + "::" + key;
        NotificationService.LockConditionPair lockConditionPair = notificationService.getOrCreateNotificationCondition(payload);

        lockConditionPair.lock.lock();
        try {
            // Check if the key is already in the database. If not, wait for the notification.
            Object value = null;

            // Initial database check
            String getSql = "SELECT value FROM %s.workflow_events WHERE workflow_uuid = ? AND key = ?";
            getSql = String.format(getSql, Constants.DB_SCHEMA);

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(getSql)) {

                stmt.setString(1, targetUuid);
                stmt.setString(2, key);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String serializedValue = rs.getString("value");
                        Object[] valueArray = JSONUtil.deserializeToArray(serializedValue);
                        value = valueArray == null ? null : valueArray[0] ;
                    }
                }
            } catch (SQLException e) {
                logger.error("Database error in getEvent initial check", e);
                throw new RuntimeException("Failed to check event", e);
            }

            if (value == null) {
                // Wait for the notification
                double actualTimeout = timeoutSeconds;
                if (callerCtx != null) {
                    // Support OAOO sleep for workflows
                    actualTimeout = sleep(
                            callerCtx.getWorkflowId(),
                            callerCtx.getTimeoutFunctionId(),
                            timeoutSeconds,
                            true // skip_sleep
                    );
                }

                try {
                    // Convert timeout to nanoseconds for await
                    long timeoutNanos = (long) (actualTimeout * 1_000_000_000L);
                    lockConditionPair.condition.await(timeoutNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for event", e);
                }

                // Read the value from the database after notification
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement stmt = conn.prepareStatement(getSql)) {

                    stmt.setString(1, targetUuid);
                    stmt.setString(2, key);

                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            String serializedValue = rs.getString("value");
                            Object[] valueArray = JSONUtil.deserializeToArray(serializedValue);
                            value = valueArray == null ? null : valueArray[0] ;
                        }
                    }
                } catch (SQLException e) {
                    logger.error("Database error in getEvent final check", e);
                    throw new RuntimeException("Failed to read event after notification", e);
                }
            }

            // Record the output if it's in a workflow
            if (callerCtx != null) {
                StepResult output = new StepResult();
                output.setWorkflowId(callerCtx.getWorkflowId());
                output.setFunctionId(callerCtx.getFunctionId());
                output.setFunctionName(functionName);
                output.setOutput(JSONUtil.serialize(value)); // null will be serialized to 'null'
                output.setError(null);

                stepsDAO.recordStepResultTxn(output);
            }

            return value;

        } finally {
            lockConditionPair.lock.unlock();
            // Remove the condition from the map after use
            notificationService.unregisterNotificationCondition(payload);
        }
    }

}
