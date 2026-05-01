package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;

public record StepResult(
    String workflowId,
    int stepId,
    String stepName,
    String output,
    String error,
    String childWorkflowId,
    String serialization) {

  public StepResult(String workflowId, int stepId, String functionName) {
    this(workflowId, stepId, functionName, null, null, null, null);
  }

  public StepResult withOutput(String v) {
    return new StepResult(workflowId, stepId, stepName, v, error, childWorkflowId, serialization);
  }

  public StepResult withError(String v) {
    return new StepResult(workflowId, stepId, stepName, output, v, childWorkflowId, serialization);
  }

  public StepResult withChildWorkflowId(String v) {
    return new StepResult(workflowId, stepId, stepName, output, error, v, serialization);
  }

  public StepResult withSerialization(String v) {
    return new StepResult(workflowId, stepId, stepName, output, error, childWorkflowId, v);
  }

  @SuppressWarnings("unchecked")
  public <R, E extends Exception> R toResult(DBOSSerializer serializer) throws E {
    if (error != null) {
      var t = SerializationUtil.deserializeError(error, serialization, serializer);
      if (t instanceof Exception) {
        throw (E) t;
      } else {
        throw new RuntimeException(t.getMessage(), t);
      }
    }

    if (output != null) {
      return (R) SerializationUtil.deserializeValue(output, serialization, serializer);
    }

    throw new IllegalStateException(
        "Recorded output and error are both null for workflow %s step %d (%s)"
            .formatted(workflowId, stepId, stepName));
  }
}
