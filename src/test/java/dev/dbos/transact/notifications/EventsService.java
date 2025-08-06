package dev.dbos.transact.notifications;

public interface EventsService {

    void setEventWorkflow(String key, Object value);

    Object getEventWorkflow(String workflowId, String key, float timeOut);

    void setMultipleEvents();

    void setWithLatch(String key, String value);

    Object getWithlatch(String workflowId, String key, float timeOut);
}
