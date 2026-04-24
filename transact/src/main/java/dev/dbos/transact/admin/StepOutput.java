package dev.dbos.transact.admin;

import dev.dbos.transact.json.JsonUtility;
import dev.dbos.transact.workflow.StepInfo;

/**
 * This record object is used only within the admin server to convert to JSON using the admin
 * server's preferred response format.
 */
record StepOutput(
    int function_id, String function_name, String output, String error, String child_workflow_id) {

  static StepOutput of(StepInfo info) {
    var output = info.output() == null ? null : JsonUtility.toJson(info.output());
    var error = info.error() == null ? null : JsonUtility.toJson(info.error());
    return new StepOutput(
        info.functionId(), info.functionName(), output, error, info.childWorkflowId());
  }
}
