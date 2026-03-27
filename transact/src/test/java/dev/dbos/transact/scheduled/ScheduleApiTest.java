package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class ScheduleApiTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
  }

  // ── Test workflow ──────────────────────────────────────────────────────────

  interface ApiTestService {
    void scheduledWork(Instant scheduledAt, Object context);
  }

  static class ApiTestServiceImpl implements ApiTestService {
    static volatile int executionCount = 0;
    static volatile Object lastContext = null;

    @Override
    @Workflow
    public void scheduledWork(Instant scheduledAt, Object context) {
      executionCount++;
      lastContext = context;
    }
  }

  private static WorkflowSchedule makeSchedule(String name) {
    return new WorkflowSchedule(
        null,
        name,
        "scheduledWork",
        ApiTestServiceImpl.class.getName(),
        "0/1 * * * * *",
        ScheduleStatus.ACTIVE,
        "{}",
        null,
        false,
        null,
        null);
  }

  // ── Tests ──────────────────────────────────────────────────────────────────

  @Test
  void testCreateAndGetSchedule() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    dbos.createSchedule(makeSchedule("create-get"));

    var result = dbos.getSchedule("create-get");
    assertTrue(result.isPresent());
    var s = result.get();
    assertEquals("create-get", s.scheduleName());
    assertEquals("scheduledWork", s.workflowName());
    assertEquals(ScheduleStatus.ACTIVE, s.status());
    assertNotNull(s.scheduleId());
    assertEquals("0/1 * * * * *", s.schedule());
  }

  @Test
  void testGetScheduleNotFound() {
    dbos.launch();
    assertTrue(dbos.getSchedule("nonexistent").isEmpty());
  }

  @Test
  void testDuplicateCreateThrows() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    dbos.createSchedule(makeSchedule("dup"));
    assertThrows(RuntimeException.class, () -> dbos.createSchedule(makeSchedule("dup")));
  }

  @Test
  void testListSchedules() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    dbos.createSchedule(makeSchedule("list-active"));
    dbos.createSchedule(makeSchedule("list-paused"));
    dbos.pauseSchedule("list-paused");

    var active = dbos.listSchedules(List.of(ScheduleStatus.ACTIVE), null, List.of("list-"));
    assertTrue(active.stream().anyMatch(s -> s.scheduleName().equals("list-active")));
    assertTrue(active.stream().noneMatch(s -> s.scheduleName().equals("list-paused")));

    var paused = dbos.listSchedules(List.of(ScheduleStatus.PAUSED), null, List.of("list-"));
    assertTrue(paused.stream().anyMatch(s -> s.scheduleName().equals("list-paused")));

    var byPrefix = dbos.listSchedules(null, null, List.of("list-"));
    assertEquals(2, byPrefix.size());

    var byWorkflow = dbos.listSchedules(null, List.of("scheduledWork"), List.of("list-"));
    assertEquals(2, byWorkflow.size());
  }

  @Test
  void testDeleteSchedule() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    dbos.createSchedule(makeSchedule("to-delete"));
    assertTrue(dbos.getSchedule("to-delete").isPresent());

    dbos.deleteSchedule("to-delete");
    assertTrue(dbos.getSchedule("to-delete").isEmpty());

    // Delete of nonexistent is a no-op
    assertDoesNotThrow(() -> dbos.deleteSchedule("to-delete"));
  }

  @Test
  void testPauseAndResumeSchedule() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    dbos.createSchedule(makeSchedule("pause-resume"));
    assertEquals(ScheduleStatus.ACTIVE, dbos.getSchedule("pause-resume").get().status());

    dbos.pauseSchedule("pause-resume");
    assertEquals(ScheduleStatus.PAUSED, dbos.getSchedule("pause-resume").get().status());

    dbos.resumeSchedule("pause-resume");
    assertEquals(ScheduleStatus.ACTIVE, dbos.getSchedule("pause-resume").get().status());
  }

  @Test
  void testApplySchedules() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    dbos.createSchedule(makeSchedule("apply-1"));
    dbos.createSchedule(makeSchedule("apply-2"));

    // Replace both with a different cron expression; also flip automaticBackfill on apply-2
    var replacement1 =
        new WorkflowSchedule(
            null,
            "apply-1",
            "scheduledWork",
            ApiTestServiceImpl.class.getName(),
            "0 0 * * * *",
            ScheduleStatus.ACTIVE,
            "{}",
            null,
            false,
            null,
            null);
    var replacement2 =
        new WorkflowSchedule(
            null,
            "apply-2",
            "scheduledWork",
            ApiTestServiceImpl.class.getName(),
            "0 0 * * * *",
            ScheduleStatus.ACTIVE,
            "{}",
            null,
            true,
            null,
            null);

    dbos.applySchedules(List.of(replacement1, replacement2));

    assertEquals("0 0 * * * *", dbos.getSchedule("apply-1").get().schedule());
    assertEquals("0 0 * * * *", dbos.getSchedule("apply-2").get().schedule());
    assertTrue(dbos.getSchedule("apply-2").get().automaticBackfill());
  }

  @Test
  void testApplySchedulesIsAtomic() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    dbos.createSchedule(makeSchedule("atomic-1"));
    dbos.createSchedule(makeSchedule("atomic-2"));

    // One valid schedule and one with an invalid cron — expect rollback; original schedules intact
    var good =
        new WorkflowSchedule(
            null,
            "atomic-1",
            "scheduledWork",
            ApiTestServiceImpl.class.getName(),
            "0 0 * * * *",
            ScheduleStatus.ACTIVE,
            "{}",
            null,
            false,
            null,
            null);
    var bad =
        new WorkflowSchedule(
            null,
            "atomic-2",
            "scheduledWork",
            ApiTestServiceImpl.class.getName(),
            "not-a-cron",
            ScheduleStatus.ACTIVE,
            "{}",
            null,
            false,
            null,
            null);

    assertThrows(Exception.class, () -> dbos.applySchedules(List.of(good, bad)));

    // Both originals should still be present with original cron
    assertEquals("0/1 * * * * *", dbos.getSchedule("atomic-1").get().schedule());
    assertEquals("0/1 * * * * *", dbos.getSchedule("atomic-2").get().schedule());
  }

  @Test
  void testTriggerSchedule() {
    var impl = new ApiTestServiceImpl();
    dbos.registerWorkflows(ApiTestService.class, impl);
    dbos.launch();

    dbos.createSchedule(makeSchedule("trigger-test"));
    var handle = dbos.triggerSchedule("trigger-test");

    assertNotNull(handle);
    assertTrue(handle.workflowId().startsWith("sched-trigger-test-trigger-"));

    var workflows = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("scheduledWork"));
    assertFalse(workflows.isEmpty());
  }

  @Test
  void testTriggerScheduleNotFound() {
    dbos.launch();
    assertThrows(IllegalArgumentException.class, () -> dbos.triggerSchedule("nonexistent"));
  }

  @Test
  void testBackfillSchedule() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    dbos.createSchedule(makeSchedule("backfill-test"));

    // Cron fires every second; backfill a 3-second window → expect 3 executions
    Instant end = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    Instant start = end.minusSeconds(3);

    var handles = dbos.backfillSchedule("backfill-test", start, end);
    assertEquals(3, handles.size());
    for (var h : handles) {
      assertTrue(h.workflowId().startsWith("sched-backfill-test-"));
    }

    // Re-running same window is idempotent (workflows already exist, just returns same IDs)
    var handles2 = dbos.backfillSchedule("backfill-test", start, end);
    assertEquals(3, handles2.size());
    assertEquals(
        handles.stream().map(h -> h.workflowId()).sorted().toList(),
        handles2.stream().map(h -> h.workflowId()).sorted().toList());
  }

  @Test
  void testBackfillScheduleNotFound() {
    dbos.launch();
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.backfillSchedule("nonexistent", Instant.now().minusSeconds(10), Instant.now()));
  }

  // ── DBOSClient tests ────────────────────────────────────────────────────────

  @Test
  void testScheduleApiViaClient() {
    dbos.launch();

    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(makeSchedule("client-test"));

      var retrieved = client.getSchedule("client-test");
      assertTrue(retrieved.isPresent());
      assertEquals("client-test", retrieved.get().scheduleName());
      assertNotNull(retrieved.get().scheduleId());

      client.pauseSchedule("client-test");
      assertEquals(ScheduleStatus.PAUSED, client.getSchedule("client-test").get().status());

      client.resumeSchedule("client-test");
      assertEquals(ScheduleStatus.ACTIVE, client.getSchedule("client-test").get().status());

      var list = client.listSchedules(null, null, List.of("client-"));
      assertFalse(list.isEmpty());

      client.deleteSchedule("client-test");
      assertTrue(client.getSchedule("client-test").isEmpty());
    }
  }

  @Test
  void testApplySchedulesViaClient() {
    dbos.launch();

    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(makeSchedule("client-apply-1"));
      client.createSchedule(makeSchedule("client-apply-2"));

      var r1 =
          new WorkflowSchedule(
              null,
              "client-apply-1",
              "scheduledWork",
              ApiTestServiceImpl.class.getName(),
              "0 0 * * * *",
              ScheduleStatus.ACTIVE,
              "{}",
              null,
              false,
              null,
              null);
      var r2 =
          new WorkflowSchedule(
              null,
              "client-apply-2",
              "scheduledWork",
              ApiTestServiceImpl.class.getName(),
              "0 0 * * * *",
              ScheduleStatus.ACTIVE,
              "{}",
              null,
              false,
              null,
              null);

      client.applySchedules(List.of(r1, r2));

      assertEquals("0 0 * * * *", client.getSchedule("client-apply-1").get().schedule());
      assertEquals("0 0 * * * *", client.getSchedule("client-apply-2").get().schedule());
    }
  }

  @Test
  void testBackfillAndTriggerViaClient() {
    dbos.registerWorkflows(ApiTestService.class, new ApiTestServiceImpl());
    dbos.launch();

    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(makeSchedule("client-backfill"));

      Instant end = Instant.now().truncatedTo(ChronoUnit.SECONDS);
      Instant start = end.minusSeconds(3);

      var handles = client.backfillSchedule("client-backfill", start, end);
      assertEquals(3, handles.size());

      var triggerHandle = client.triggerSchedule("client-backfill");
      assertNotNull(triggerHandle);
      assertTrue(triggerHandle.workflowId().startsWith("sched-client-backfill-trigger-"));
    }
  }
}
