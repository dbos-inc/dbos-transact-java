package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.StepInfo;

import java.util.Collections;
import java.util.List;

public class ListStepsResponse extends BaseResponse {
  public List<Step> output;

  public static class Step {
    public int function_id;
    public String function_name;
    public String output;
    public String error;
    public String child_workflow_id;

    public Step(StepInfo info) {
      Object output = info.getOutput();
      var error = info.getError();

      this.function_id = info.getFunctionId();
      this.function_name = info.getFunctionName();
      this.output = output != null ? JSONUtil.toJson(output) : null;
      this.error =
          error != null ? String.format("%s: %s", error.className(), error.message()) : null;
      this.child_workflow_id = info.getChildWorkflowId();
    }
  }

  public ListStepsResponse(BaseMessage message, List<Step> output) {
    super(message.type, message.request_id);
    this.output = output;
  }

  public ListStepsResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.output = Collections.emptyList();
  }
}
