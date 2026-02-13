package dev.dbos.transact.tempworkflows;

import dev.dbos.transact.workflow.SerializationStrategy;

public interface InternalWorkflowsService {

  void sendWorkflow(
      String destinationId, Object message, String topic, SerializationStrategy serialization);
}
