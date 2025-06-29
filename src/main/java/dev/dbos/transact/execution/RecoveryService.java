package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RecoveryService {

    private final SystemDatabase systemDatabase;
    private final DBOSExecutor dbosExecutor ;
    public Logger logger = LoggerFactory.getLogger(RecoveryService.class);

    public RecoveryService(DBOSExecutor dbosExecutor, SystemDatabase systemDatabase) {
        this.systemDatabase = systemDatabase ;
        this.dbosExecutor = dbosExecutor ;
    }

    public void recoverWorkflows() {

        try {
            List<GetPendingWorkflowsOutput> pendingWorkflowsOutputs = getPendingWorkflows();
            recoverWorkflows(pendingWorkflowsOutputs);
        } catch(SQLException e) {
            logger.error("Recovery could not complete due to SQL error",e);
        }

    }

    public List<WorkflowHandle> recoverWorkflows(List<GetPendingWorkflowsOutput> pendingWorkflowsOutputs )  {

        List<WorkflowHandle> handles = new ArrayList<>();

        for (GetPendingWorkflowsOutput pendingW : pendingWorkflowsOutputs) {
            logger.info("Recovery executing workflow " + pendingW.getWorkflowUuid());
            handles.add(dbosExecutor.executeWorkflowById(pendingW.getWorkflowUuid()));
        }

        return handles ;

    }

    public List<GetPendingWorkflowsOutput> getPendingWorkflows() throws SQLException {
        return systemDatabase
                .getPendingWorkflows(Constants.DEFAULT_EXECUTORID,
                        Constants.DEFAULT_APP_VERSION);
    }
}
