package dev.dbos.transact.tempworkflows;

public interface InternalWorkflowsService {

  void sendWorkflow(String destinationId, Object message, String topic);
}
