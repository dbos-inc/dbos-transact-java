package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowSchedule;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A schedule written from a workflow commits with its checkpoint, and a refusal is recorded as the
 * step's answer, so a replay is refused the same way after the obstacle is gone.
 */
class ScheduleCheckpointTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;

  private ScheduleCreator proxy;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    dbos.registerProxy(ScheduledWorkflowService.class, new ScheduledWorkflowImpl());
    proxy = dbos.registerProxy(ScheduleCreator.class, new ScheduleCreatorImpl(dbos));
    dbos.launch();
  }

  static WorkflowSchedule schedule(String name) {
    return new WorkflowSchedule(
        name, "scheduledRun", ScheduledWorkflowImpl.class.getName(), "0 0 * * * *");
  }

  @Test
  public void aRefusedCreateReplaysItsRefusalOnceTheNameIsFree() throws Exception {
    var name = "taken-%d".formatted(System.nanoTime());
    dbos.createSchedule(schedule(name));
    var workflowId = "creator-%d".formatted(System.nanoTime());

    var result =
        dbos.startWorkflow(() -> proxy.creates(name), new StartWorkflowOptions(workflowId))
            .getResult();
    assertEquals("refused", result);
    var steps = dbos.listWorkflowSteps(workflowId);
    assertEquals(1, steps.size());
    assertEquals("DBOS.createSchedule", steps.get(0).functionName());
    assertNotNull(steps.get(0).error(), "the refusal is the step's answer");

    // With the holder gone, a create would now succeed; the fork replays the recorded refusal.
    dbos.deleteSchedule(name);
    assertEquals("refused", dbos.<String, RuntimeException>forkWorkflow(workflowId, 1).getResult());
    assertTrue(dbos.getSchedule(name).isEmpty(), "the replay created nothing");
  }
}

interface ScheduleCreator {
  String creates(String name);
}

class ScheduleCreatorImpl implements ScheduleCreator {
  private final DBOS dbos;

  ScheduleCreatorImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String creates(String name) {
    try {
      dbos.createSchedule(ScheduleCheckpointTest.schedule(name));
      return "created";
    } catch (RuntimeException e) {
      return "refused";
    }
  }
}
