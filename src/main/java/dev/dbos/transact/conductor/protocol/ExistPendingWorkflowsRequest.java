package dev.dbos.transact.conductor.protocol;

public class ExistPendingWorkflowsRequest extends BaseMessage {
    public String executor_id;
    public String application_version;

    public ExistPendingWorkflowsRequest() {

    }

    public ExistPendingWorkflowsRequest(String requestId, String executorId, String appVer) {
        this.type = MessageType.EXIST_PENDING_WORKFLOWS.getValue();
        this.request_id = requestId;
        this.executor_id = executorId;
        this.application_version = appVer;
    }
}
