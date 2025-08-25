package dev.dbos.transact;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.scheduled.SchedulerService;

// Helper class to retrieve DBOS internals via package private methods
public class DBOSUtils {
    public static SystemDatabase getSystemDatabase(DBOS dbos) {
        return dbos.getSystemDatabase();
    }

    public static DBOSExecutor getDbosExecutor(DBOS dbos) {
        return dbos.getDbosExecutor();
    }

    public static QueueService getQueueService(DBOS dbos) {
        return dbos.getQueueService();
    }

    public static SchedulerService getSchedulerService(DBOS dbos) {
        return dbos.getSchedulerService();
    }

}
