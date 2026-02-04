package dev.dbos.transact.tempworkflows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Workflow;

public class InternalWorkflowsServiceImpl implements InternalWorkflowsService {
  @Workflow(name = "internalSendWorkflow")
  public void sendWorkflow(
      String destinationId, Object message, String topic, String serialization) {
    // Convert the format name back to SerializationStrategy for the public API
    SerializationStrategy strategy = null;
    if (serialization != null) {
      if ("portable_json".equals(serialization)) {
        strategy = SerializationStrategy.PORTABLE;
      } else if ("java_jackson".equals(serialization)) {
        strategy = SerializationStrategy.NATIVE;
      }
    }
    DBOS.send(destinationId, message, topic, null, strategy);
  }
}
