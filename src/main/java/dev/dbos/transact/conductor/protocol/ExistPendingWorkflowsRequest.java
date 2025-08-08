package dev.dbos.transact.conductor.protocol;

public class ExistPendingWorkflowsRequest extends BaseMessage {
    public String executor_id;
    public String application_version;
}
