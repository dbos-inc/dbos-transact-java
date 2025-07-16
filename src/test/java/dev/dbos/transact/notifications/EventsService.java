package dev.dbos.transact.notifications;

public interface EventsService {

    void setEventWorkflow(String key, Object value) ;

    Object getEventWorkflow(String workflowId, String key, float timeOut) ;
}
