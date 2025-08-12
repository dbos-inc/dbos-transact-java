package dev.dbos.transact.conductor.protocol;

public class ExecutorInfoResponse extends BaseResponse {
    public String executor_id;
    public String application_version;
    public String hostname;

    public ExecutorInfoResponse(BaseMessage message, String executorId, String appVersion, String hostName) {
        super(MessageType.EXECUTOR_INFO.getValue(), message.request_id);
        this.executor_id = executorId;
        this.application_version = appVersion;
        this.hostname = hostName;
    }

    public ExecutorInfoResponse(BaseMessage message, Exception ex) {
        super(message.type, message.request_id, ex.getMessage());
    }

}
