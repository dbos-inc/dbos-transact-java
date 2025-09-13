package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.*;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type",
    visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = ExecutorInfoRequest.class, name = "executor_info"),
  @JsonSubTypes.Type(value = RecoveryRequest.class, name = "recovery"),
  @JsonSubTypes.Type(value = CancelRequest.class, name = "cancel"),
  @JsonSubTypes.Type(value = ResumeRequest.class, name = "resume"),
  @JsonSubTypes.Type(value = RestartRequest.class, name = "restart"),
  @JsonSubTypes.Type(value = ForkWorkflowRequest.class, name = "fork_workflow"),
  @JsonSubTypes.Type(value = ListWorkflowsRequest.class, name = "list_workflows"),
  @JsonSubTypes.Type(value = ListQueuedWorkflowsRequest.class, name = "list_queued_workflows"),
  @JsonSubTypes.Type(value = GetWorkflowRequest.class, name = "get_workflow"),
  @JsonSubTypes.Type(value = ExistPendingWorkflowsRequest.class, name = "exist_pending_workflows"),
  @JsonSubTypes.Type(value = ListStepsRequest.class, name = "list_steps"),
  @JsonSubTypes.Type(value = RetentionRequest.class, name = "retention"),
})
public abstract class BaseMessage {
  public String type;
  public String request_id;
}
