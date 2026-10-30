package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

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

  @Test
  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT)
  public void aCreateThatFailsBeforeCommitLeavesNoSchedule() throws Exception {
    var name = "crashed-%d".formatted(System.nanoTime());
    var workflowId = "creator-%d".formatted(System.nanoTime());
    var fired = new AtomicBoolean();
    // Armed for this workflow only: the trigger is global, and other classes' steps pass it.
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (workflowId.equals(DBOSContextHolder.get().getWorkflowId())
                      && fired.compareAndSet(false, true)) {
                    throw new RuntimeException("crash");
                  }
                }));
    String result;
    try {
      result =
          dbos.startWorkflow(() -> proxy.creates(name), new StartWorkflowOptions(workflowId))
              .getResult();
    } finally {
      DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT);
    }

    assertEquals("refused", result);
    assertTrue(fired.get());
    assertTrue(dbos.getSchedule(name).isEmpty(), "the schedule rolled back with the step");
    assertTrue(dbos.listWorkflowSteps(workflowId).isEmpty(), "and so did its checkpoint");
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
