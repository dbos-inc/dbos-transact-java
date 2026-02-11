package dev.dbos.transact.workflow;

import java.util.List;

public record ExportedWorkflow(
    WorkflowStatus status,
    List<StepInfo> steps,
    List<WorkflowEvent> events,
    List<WorkflowEventHistory> eventHistory,
    List<WorkflowStream> streams) {}
