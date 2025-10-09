package dev.dbos.transact.notifications;

import java.time.Duration;

public interface EventsService {

  void setEventWorkflow(String key, Object value);

  Object getEventWorkflow(String workflowId, String key, Duration timeout);

  void setMultipleEvents();

  void setWithLatch(String key, String value);

  Object getWithlatch(String workflowId, String key, Duration timeOut);
}
