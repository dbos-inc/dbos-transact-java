package dev.dbos.transact.devhawk;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class HawkServiceImpl implements HawkService {
  private HawkService proxy;

  public void setProxy(HawkService proxy) {
    this.proxy = proxy;
  }

  @Workflow
  @Override
  public String simpleWorkflow(String workflowId) {
    validateWorkflowId(workflowId);
    return LocalDate.now().format(DateTimeFormatter.ISO_DATE);
  }

  @Workflow
  @Override
  public String sleepWorkflow(long sleepSeconds) {
    sleep(Duration.ofSeconds(sleepSeconds));
    return LocalDate.now().format(DateTimeFormatter.ISO_DATE);
  }

  @Workflow
  @Override
  public String parentWorkflow(String workflowId) {
    var childId = workflowId == null ? null : "%s-0".formatted(workflowId);
    return proxy.simpleWorkflow(childId);
  }

  @Workflow
  @Override
  public String parentSleepWorkflow(long sleepSeconds) {
    try (var o = new WorkflowOptions().withTimeout(null).setContext()) {
      return proxy.sleepWorkflow(sleepSeconds);
    }
  }

  private void validateWorkflowId(String workflowId) {
    if (workflowId != null) {
      var ctxWfId = DBOSContext.workflowId().get();
      if (!workflowId.equals(ctxWfId)) {
        throw new RuntimeException("workflow ID mismatch %s %s".formatted(workflowId, ctxWfId));
      }
    }
  }

  private void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
