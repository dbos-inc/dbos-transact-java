package dev.dbos.transact.tempworkflows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

public class InternalWorkflowsServiceImpl implements InternalWorkflowsService {
  @Workflow(name = "internalSendWorkflow")
  public void sendWorkflow(String destinationId, Object message, String topic) {
    DBOS.send(destinationId, message, topic);
  }
}
