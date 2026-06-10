package dev.dbos.transact.queue;

public interface QueueChildService {
  String parentWorkflow(String v1, String v2);

  String childWorkflow(String val);
}
