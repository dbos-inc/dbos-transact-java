package dev.dbos.transact.notifications;

import java.time.Duration;

public interface EventsService {

  String setEventWorkflow(String key, String value);

  Object getEventWorkflow(String workflowId, String key, Duration timeout);

  void setMultipleEvents();

  void setWithLatch(String key, String value);

  Object getWithlatch(String workflowId, String key, Duration timeOut);

  String getEventTwice(String wfid, String key) throws InterruptedException;

  void setEventTwice(String key, String v1, String v2) throws InterruptedException;
}
