package dev.dbos.transact.conductor.protocol;

public class ExecutorInfoRequest extends BaseMessage {
    // empty on purpose

    public ExecutorInfoRequest() {
    }

    public ExecutorInfoRequest(String requestId) {
        this.type = MessageType.EXECUTOR_INFO.getValue();
        this.request_id = requestId;
    }
}
