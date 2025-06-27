package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class RecoveryService {

    private final SystemDatabase systemDatabase;
    private final DBOSExecutor dbosExecutor ;
    public Logger logger = LoggerFactory.getLogger(RecoveryService.class);

    public RecoveryService(DBOSExecutor dbosExecutor, SystemDatabase systemDatabase) {
        this.systemDatabase = systemDatabase ;
        this.dbosExecutor = dbosExecutor ;
    }

    public void recover_workflows() {

        try {
            List<GetPendingWorkflowsOutput> pendingWorkflowsOutputs = systemDatabase
                    .getPendingWorkflows(Constants.TEMP_HARDCODED_EXECUTORID,
                            Constants.TEMP_HARDCODED_APP_VERSION);


            for (GetPendingWorkflowsOutput pendingW : pendingWorkflowsOutputs) {
                logger.info("Recovery executing workflow " + pendingW.getWorkflowUuid());
                dbosExecutor.executeWorkflowById(pendingW.getWorkflowUuid());
            }


        } catch(SQLException e) {
            logger.error("Recovery could not complete due to SQL error",e);
        }

    }
}
