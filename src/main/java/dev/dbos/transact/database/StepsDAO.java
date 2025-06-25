package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.UnExpectedStepException;
import dev.dbos.transact.exceptions.WorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSWorkflowConflictException;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Objects;

public class StepsDAO {

    private Logger logger = LoggerFactory.getLogger(StepsDAO.class);
    private DataSource dataSource ;

    StepsDAO(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void recordStepResultTxn(StepResult result) throws SQLException
    {

        String sql = String.format(
                "INSERT INTO %s.operation_outputs (workflow_uuid, function_id, function_name, output, error) " +
                        "VALUES (?, ?, ?, ?, ?)",
                Constants.DB_SCHEMA
        );

        try (Connection connection = dataSource.getConnection() ;
             PreparedStatement pstmt = connection.prepareStatement(sql)) {
            int paramIdx = 1;
            pstmt.setString(paramIdx++, result.getWorkflowId());
            pstmt.setInt(paramIdx++, result.getFunctionId());
            pstmt.setString(paramIdx++, result.getFunctionName());

            if (result.getOutput() != null) {
                pstmt.setString(paramIdx++, result.getOutput());
            } else {
                pstmt.setNull(paramIdx++, Types.LONGVARCHAR);
            }

            if (result.getError() != null) {
                pstmt.setString(paramIdx++, result.getError());
            } else {
                pstmt.setNull(paramIdx++, Types.LONGVARCHAR);
            }

            pstmt.executeUpdate();

        } catch (SQLException e) {
            if ("23505".equals(e.getSQLState())) {
                throw new DBOSWorkflowConflictException(result.getWorkflowId(),
                        String.format("Workflow %s already exists", result.getWorkflowId()));
            } else {
                throw e;
            }
        }
    }

    /**
     * Checks the execution status and output of a specific operation within a workflow.
     * This method corresponds to Python's '_check_operation_execution_txn'.
     *
     * @param workflowId The UUID of the workflow.
     * @param functionId The ID of the function/operation.
     * @param functionName The expected name of the function/operation.
     * @param connection The active JDBC connection (corresponding to Python's 'conn: sa.Connection').
     * @return A {@link StepResult} object if the operation has completed, otherwise {@code null}.
     * @throws IllegalStateException If the workflow does not exist in the status table.
     * @throws WorkflowCancelledException If the workflow is in a cancelled status.
     * @throws UnExpectedStepException If the recorded function name for the operation does not match the provided name.
     * @throws SQLException For other database access errors.
     */
    public StepResult checkStepExecutionTxn(
            String workflowId,
            int functionId,
            String functionName,
            Connection connection
    ) throws SQLException, IllegalStateException, WorkflowCancelledException, UnExpectedStepException {

        // --- First query: Retrieve the workflow status ---
        String workflowStatusSql = String.format(
                "SELECT status FROM %s.workflow_status WHERE workflow_uuid = ?",
                Constants.DB_SCHEMA
        );

        String workflowStatus = null;
        try (PreparedStatement pstmt = connection.prepareStatement(workflowStatusSql)) {
            pstmt.setString(1, workflowId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    workflowStatus = rs.getString("status");
                }
            }
        }

        // Check if the workflow exists (mimics Python's assert)
        if (workflowStatus == null) {
            throw new IllegalStateException(String.format("Error: Workflow %s does not exist", workflowId));
        }

        // If the workflow is cancelled, raise the exception
        if (Objects.equals(workflowStatus, WorkflowState.CANCELLED.name())) {
            throw new WorkflowCancelledException(
                    String.format("Workflow %s is cancelled. Aborting function.", workflowId)
            );
        }

        // --- Second query: Retrieve operation outputs ---
        String operationOutputSql = String.format(
                "SELECT output, error, function_name " +
                        "FROM %s.operation_outputs " +
                        "WHERE workflow_uuid = ? AND function_id = ?",
                Constants.DB_SCHEMA
        );

        StepResult recordedResult = null;
        String recordedFunctionName = null;

        try (PreparedStatement pstmt = connection.prepareStatement(operationOutputSql)) {
            pstmt.setString(1, workflowId);
            pstmt.setInt(2, functionId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) { // Check if any operation output row exists
                    String output = rs.getString("output");
                    String error = rs.getString("error");
                    recordedFunctionName = rs.getString("function_name");
                    recordedResult = new StepResult(workflowId, functionId, recordedFunctionName, output, error);
                }
            }
        }

        // If there are no operation outputs, return None (mimics Python's 'if not operation_output_rows')
        if (recordedResult == null) {
            return null;
        }

        // If the provided and recorded function name are different, throw an exception
        if (!Objects.equals(functionName, recordedFunctionName)) {
            throw new UnExpectedStepException(
                    workflowId,
                    functionId,
                    functionName,
                    recordedFunctionName
            );
        }

        return recordedResult;
    }
}


