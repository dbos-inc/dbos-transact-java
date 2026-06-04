package dev.dbos.transact.queue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;

public class QueueChildServiceImpl implements QueueChildService {

  private final DBOS dbos;
  private QueueChildService self;

  public QueueChildServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  public void setSelf(QueueChildService self) {
    this.self = self;
  }

  @Override
  @Workflow
  public String parentWorkflow(String v1, String v2) {
    var handles = new ArrayList<WorkflowHandle<String, ?>>();
    for (var val : new String[] {v1, v2, v1, v2}) {
      handles.add(
          dbos.startWorkflow(
              () -> self.childWorkflow(val), new StartWorkflowOptions().withQueue("child_queue")));
    }

    // With concurrency=3 and 4 children, the 4th should still be ENQUEUED at this point.
    var h4 = handles.get(3);
    dbos.runStep(
        () -> {
          var status = dbos.retrieveWorkflow(h4.workflowId()).getStatus();
          if (status == null || !WorkflowState.ENQUEUED.equals(status.status())) {
            throw new IllegalStateException(
                "Expected h4 to be ENQUEUED, got: " + (status == null ? "null" : status.status()));
          }
          return null;
        },
        "verifyH4Enqueued");

    // Release all children via events.
    for (var h : handles) {
      dbos.send(h.workflowId(), "go", "release");
    }

    var sb = new StringBuilder();
    for (var h : handles) {
      try {
        sb.append(h.getResult());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return sb.toString();
  }

  @Override
  @Workflow
  public String childWorkflow(String val) {
    Optional<String> signal = dbos.recv("release", Duration.ofSeconds(30));
    if (signal.isEmpty()) {
      throw new RuntimeException("Timed out waiting for release signal");
    }
    return val + "d";
  }
}
