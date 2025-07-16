package dev.dbos.transact.notifications;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

public class EventsServiceImpl implements EventsService {

    private final DBOS dbos ;

    EventsServiceImpl(DBOS dbos) {
        this.dbos = dbos;
    }

    @Workflow(name = "setEventWorkflow")
    public void setEventWorkflow(String key, Object value) {
        dbos.setEvent(key, value);
    }

    @Workflow(name = "getEventWorkflow")
    public Object getEventWorkflow(String workflowId, String key, float timeOut) {
        return dbos.getEvent(workflowId, key, timeOut) ;
    }

    @Workflow(name = "setMultipleEvents")
    public void setMultipleEvents() {
        dbos.setEvent("key1","value1");
        dbos.setEvent("key2", Double.valueOf(241.5));
        dbos.setEvent("key3", null);
    }
}
