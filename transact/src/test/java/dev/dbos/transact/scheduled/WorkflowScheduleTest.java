package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class WorkflowScheduleTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig =
        pgContainer.dbosConfig().withSchedulerPollingInterval(Duration.ofSeconds(1));
    dataSource = pgContainer.dataSource();
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private ScheduledWorkflowImpl registerAndLaunch() {
    dbos = new DBOS(dbosConfig);
    var impl = new ScheduledWorkflowImpl();
    dbos.registerWorkflows(ScheduledWorkflowService.class, impl);
    dbos.launch();
    return impl;
  }

  private static String workflowName() {
    return "scheduledRun";
  }

  private static String className() {
    return ScheduledWorkflowImpl.class.getName();
  }

  // ── createSchedule ────────────────────────────────────────────────────────

  @Test
  public void createAndGetSchedule() {
    registerAndLaunch();

    dbos.createSchedule(
        "my-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);

    var s = dbos.getSchedule("my-sched").orElseThrow();
    assertEquals("my-sched", s.scheduleName());
    assertEquals(workflowName(), s.workflowName());
    assertEquals(className(), s.className());
    assertEquals("0/5 * * * * *", s.cron());
    assertEquals(ScheduleStatus.ACTIVE, s.status());
    assertNull(s.lastFiredAt());
    assertFalse(s.automaticBackfill());
    assertNull(s.cronTimezone());
    assertNull(s.queueName());
    assertNotNull(s.id());
  }

  @Test
  public void createScheduleWithAllFields() {
    dbos = new DBOS(dbosConfig);
    dbos.registerQueue(new dev.dbos.transact.workflow.Queue("sched-q").withConcurrency(1));
    dbos.registerWorkflows(ScheduledWorkflowService.class, new ScheduledWorkflowImpl());
    dbos.launch();

    dbos.createSchedule(
        "full-sched",
        workflowName(),
        className(),
        "0 0 * * * *",
        "{\"key\":\"val\"}",
        true,
        ZoneId.of("America/New_York"),
        "sched-q");

    var s = dbos.getSchedule("full-sched").orElseThrow();
    assertEquals("{\"key\":\"val\"}", s.context());
    assertTrue(s.automaticBackfill());
    assertEquals(ZoneId.of("America/New_York"), s.cronTimezone());
    assertEquals("sched-q", s.queueName());
  }

  @Test
  public void createScheduleDuplicate() {
    registerAndLaunch();
    dbos.createSchedule(
        "dup-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
    assertThrows(
        RuntimeException.class,
        () ->
            dbos.createSchedule(
                "dup-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null));
  }

  @Test
  public void createScheduleInvalidCron() {
    registerAndLaunch();
    assertThrows(
        RuntimeException.class,
        () ->
            dbos.createSchedule(
                "bad-cron", workflowName(), className(), "not-a-cron", null, false, null, null));
  }

  @Test
  public void createScheduleUnknownWorkflow() {
    registerAndLaunch();
    assertThrows(
        IllegalStateException.class,
        () ->
            dbos.createSchedule(
                "bad-wf", "noSuchMethod", className(), "0/5 * * * * *", null, false, null, null));
  }

  @Test
  public void createScheduleUnknownQueue() {
    registerAndLaunch();
    assertThrows(
        IllegalStateException.class,
        () ->
            dbos.createSchedule(
                "bad-q",
                workflowName(),
                className(),
                "0/5 * * * * *",
                null,
                false,
                null,
                "no-such-queue"));
  }

  // ── getSchedule ───────────────────────────────────────────────────────────

  @Test
  public void getScheduleNotFound() {
    registerAndLaunch();
    assertTrue(dbos.getSchedule("nonexistent").isEmpty());
  }

  // ── deleteSchedule ────────────────────────────────────────────────────────

  @Test
  public void deleteSchedule() {
    registerAndLaunch();
    dbos.createSchedule(
        "del-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
    assertTrue(dbos.getSchedule("del-sched").isPresent());

    dbos.deleteSchedule("del-sched");
    assertTrue(dbos.getSchedule("del-sched").isEmpty());
  }

  @Test
  public void deleteScheduleNotFound() {
    registerAndLaunch();
    assertDoesNotThrow(() -> dbos.deleteSchedule("nonexistent"));
  }

  // ── pauseSchedule / resumeSchedule ────────────────────────────────────────

  @Test
  public void pauseAndResumeSchedule() {
    registerAndLaunch();
    dbos.createSchedule(
        "pause-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);

    dbos.pauseSchedule("pause-sched");
    assertEquals(ScheduleStatus.PAUSED, dbos.getSchedule("pause-sched").orElseThrow().status());

    dbos.resumeSchedule("pause-sched");
    assertEquals(ScheduleStatus.ACTIVE, dbos.getSchedule("pause-sched").orElseThrow().status());
  }

  // ── listSchedules ─────────────────────────────────────────────────────────

  @Test
  public void listSchedules() {
    registerAndLaunch();

    dbos.createSchedule(
        "alpha-1", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
    dbos.createSchedule(
        "alpha-2", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
    dbos.createSchedule(
        "beta-1", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
    dbos.pauseSchedule("beta-1");

    // no filter
    assertEquals(3, dbos.listSchedules(null, null, null).size());

    // filter by status
    assertEquals(2, dbos.listSchedules(List.of(ScheduleStatus.ACTIVE), null, null).size());
    assertEquals(1, dbos.listSchedules(List.of(ScheduleStatus.PAUSED), null, null).size());
    assertEquals(
        3,
        dbos.listSchedules(List.of(ScheduleStatus.ACTIVE, ScheduleStatus.PAUSED), null, null)
            .size());

    // filter by workflow name
    assertEquals(3, dbos.listSchedules(null, List.of(workflowName()), null).size());
    assertEquals(0, dbos.listSchedules(null, List.of("unknownWorkflow"), null).size());

    // filter by name prefix
    assertEquals(2, dbos.listSchedules(null, null, List.of("alpha-")).size());
    assertEquals(1, dbos.listSchedules(null, null, List.of("beta-")).size());
    assertEquals(3, dbos.listSchedules(null, null, List.of("alpha-", "beta-")).size());
    assertEquals(0, dbos.listSchedules(null, null, List.of("gamma-")).size());

    // combined status + prefix
    assertEquals(
        2, dbos.listSchedules(List.of(ScheduleStatus.ACTIVE), null, List.of("alpha-")).size());
    assertEquals(
        0, dbos.listSchedules(List.of(ScheduleStatus.PAUSED), null, List.of("alpha-")).size());
  }

  // ── applySchedules ────────────────────────────────────────────────────────

  @Test
  public void applySchedulesCreatesAndReplaces() {
    registerAndLaunch();

    dbos.createSchedule(
        "apply-1", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
    assertEquals(1, dbos.listSchedules(null, null, null).size());

    // apply-1 is replaced (new cron) and apply-2 is created — applySchedules upserts, it does
    // not delete schedules absent from the list.
    dbos.applySchedules(
        List.of(
            new WorkflowSchedule(
                null, "apply-1", workflowName(), className(), "0/10 * * * * *",
                ScheduleStatus.ACTIVE, null, null, false, null, null),
            new WorkflowSchedule(
                null, "apply-2", workflowName(), className(), "0/5 * * * * *",
                ScheduleStatus.ACTIVE, null, null, false, null, null)));

    assertEquals(2, dbos.listSchedules(null, null, null).size());
    assertEquals("0/10 * * * * *", dbos.getSchedule("apply-1").orElseThrow().cron());
    assertTrue(dbos.getSchedule("apply-2").isPresent());
  }

  @Test
  public void applySchedulesAlwaysCreatesActive() {
    // applySchedules ignores the status field in the input — it always creates ACTIVE
    registerAndLaunch();

    dbos.applySchedules(
        List.of(
            new WorkflowSchedule(
                null, "apply-paused", workflowName(), className(), "0/5 * * * * *",
                ScheduleStatus.PAUSED, null, null, false, null, null)));

    assertEquals(
        ScheduleStatus.ACTIVE, dbos.getSchedule("apply-paused").orElseThrow().status());
  }

  // ── triggerSchedule ───────────────────────────────────────────────────────

  @Test
  public void triggerSchedule() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        "trigger-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);

    var handle = dbos.<Void, RuntimeException>triggerSchedule("trigger-sched");
    handle.getResult();

    assertEquals(1, impl.counter);
    assertNotNull(impl.lastScheduled);
  }

  @Test
  public void triggerSchedulePassesContext() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        "ctx-sched", workflowName(), className(), "0/5 * * * * *", "my-context", false, null, null);

    dbos.<Void, RuntimeException>triggerSchedule("ctx-sched").getResult();
    assertEquals("my-context", impl.lastContext);
  }

  @Test
  public void triggerScheduleNotFound() {
    registerAndLaunch();
    assertThrows(
        IllegalStateException.class, () -> dbos.triggerSchedule("no-such-sched"));
  }

  // ── backfillSchedule ──────────────────────────────────────────────────────

  @Test
  public void backfillSchedule() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        "backfill-sched", workflowName(), className(), "0/1 * * * * *", null, false, null, null);

    // Window (start, end] exclusive start, inclusive end → T+1s, T+2s, T+3s = 3 executions
    var start = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    var end = start.plusSeconds(3);

    var handles = dbos.backfillSchedule("backfill-sched", start, end);

    assertEquals(3, handles.size());
    for (var h : handles) {
      h.getResult();
    }
    assertEquals(3, impl.counter);
  }

  @Test
  public void backfillScheduleEmptyWindow() {
    registerAndLaunch();

    dbos.createSchedule(
        "backfill-empty", workflowName(), className(), "0/1 * * * * *", null, false, null, null);

    // end == start → no executions (nextExecution(T) = T+1s which is after T)
    var t = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    var handles = dbos.backfillSchedule("backfill-empty", t, t);
    assertEquals(0, handles.size());
  }

  @Test
  public void backfillScheduleNotFound() {
    registerAndLaunch();
    var t = Instant.now();
    assertThrows(
        IllegalStateException.class,
        () -> dbos.backfillSchedule("no-such-sched", t, t.plusSeconds(10)));
  }

  // ── End-to-end ────────────────────────────────────────────────────────────

  @Test
  public void scheduleRunsAfterPolling() throws Exception {
    var impl = registerAndLaunch();

    // With 1s polling, the scheduler will pick this up within ~1 second of creation.
    dbos.createSchedule(
        "run-sched", workflowName(), className(), "0/1 * * * * *", null, false, null, null);

    // Allow time for at least 2 scheduler polls + a few workflow executions
    Thread.sleep(5000);

    assertTrue(impl.counter >= 2, "Expected at least 2 executions, got " + impl.counter);
    assertTrue(impl.counter <= 6, "Expected at most 6 executions, got " + impl.counter);
  }
}
