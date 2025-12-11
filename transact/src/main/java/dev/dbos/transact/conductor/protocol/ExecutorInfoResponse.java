package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.DBOS;

public class ExecutorInfoResponse extends BaseResponse {
  public String executor_id;
  public String application_version;
  public String hostname;
  public String language;
  public String dbos_version;

  public ExecutorInfoResponse(
      BaseMessage message, String executorId, String appVersion, String hostName) {
    super(MessageType.EXECUTOR_INFO.getValue(), message.request_id);
    this.executor_id = executorId;
    this.application_version = appVersion;
    this.hostname = hostName;
    this.language = "java";
    this.dbos_version = DBOS.version();
  }

  public ExecutorInfoResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
  }
}
