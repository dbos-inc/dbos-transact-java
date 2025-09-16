package dev.dbos.transact.context;

public record WorkflowInfo(String workflowId, int functionId) {
    public static WorkflowInfo fromContext() {
      DBOSContext ctx = DBOSContextHolder.get();
      return ctx.hasParent()
          ? new WorkflowInfo(ctx.getParentWorkflowId(), ctx.getParentFunctionId())
          : null;
    }

    public static WorkflowInfo fromContext(DBOSContext ctx) {
      return ctx.hasParent()
          ? new WorkflowInfo(ctx.getParentWorkflowId(), ctx.getParentFunctionId())
          : null;
    }
  }