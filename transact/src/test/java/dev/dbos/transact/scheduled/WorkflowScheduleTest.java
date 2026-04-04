package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    dbosConfig = pgContainer.dbosConfig().withSchedulerPollingInterval(Duration.ofSeconds(1));
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
                "dup-sched",
                workflowName(),
                className(),
                "0/5 * * * * *",
                null,
                false,
                null,
                null));
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
                null,
                "apply-1",
                workflowName(),
                className(),
                "0/10 * * * * *",
                ScheduleStatus.ACTIVE,
                null,
                null,
                false,
                null,
                null),
            new WorkflowSchedule(
                null,
                "apply-2",
                workflowName(),
                className(),
                "0/5 * * * * *",
                ScheduleStatus.ACTIVE,
                null,
                null,
                false,
                null,
                null)));

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
                null,
                "apply-paused",
                workflowName(),
                className(),
                "0/5 * * * * *",
                ScheduleStatus.PAUSED,
                null,
                null,
                false,
                null,
                null)));

    assertEquals(ScheduleStatus.ACTIVE, dbos.getSchedule("apply-paused").orElseThrow().status());
  }

  // ── triggerSchedule ───────────────────────────────────────────────────────

  @Test
  public void triggerSchedule() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        "trigger-sched",
        workflowName(),
        className(),
        "0 0 0 1 1 *",
        null,
        false,
        null,
        null); // Jan 1st only

    impl.reset();
    var handle = dbos.<Void, RuntimeException>triggerSchedule("trigger-sched");
    handle.getResult();

    // At least 1 execution from trigger, possibly more from scheduler
    assertTrue(impl.counter >= 1, "Expected at least 1 execution, got " + impl.counter);
    assertNotNull(impl.lastScheduled);
  }

  @Test
  public void triggerSchedulePassesContext() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        "ctx-sched", workflowName(), className(), "0 0 0 1 1 *", "my-context", false, null, null);

    impl.reset();
    dbos.<Void, RuntimeException>triggerSchedule("ctx-sched").getResult();
    // At least 1 execution from trigger
    assertTrue(impl.counter >= 1, "Expected at least 1 execution, got " + impl.counter);
    assertEquals("my-context", impl.lastContext);
  }

  @Test
  public void triggerScheduleNotFound() {
    registerAndLaunch();
    assertThrows(IllegalStateException.class, () -> dbos.triggerSchedule("no-such-sched"));
  }

  // ── backfillSchedule ──────────────────────────────────────────────────────

  @Test
  public void backfillSchedule() throws Exception {
    var impl = registerAndLaunch();

    // Use a cron that won't fire during the test (every minute)
    dbos.createSchedule(
        "backfill-sched", workflowName(), className(), "0 * * * * *", null, false, null, null);

    // Window (start, end] exclusive start, inclusive end → T+1min, T+2min, T+3min = 3 executions
    var start = Instant.parse("2024-01-01T10:00:30Z");
    var end = Instant.parse("2024-01-01T10:03:30Z");

    impl.reset();
    var handles = dbos.backfillSchedule("backfill-sched", start, end);

    assertEquals(3, handles.size());
    for (var h : handles) {
      h.getResult();
    }
    // Backfill creates exactly 3, scheduler may add more
    assertTrue(impl.counter >= 3, "Expected at least 3 executions, got " + impl.counter);
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

  @Test
  public void backfillScheduleCorrectTimes() throws Exception {
    var impl = registerAndLaunch();

    // Pause scheduler to prevent interference with backfill results
    DBOSTestAccess.getSchedulerService(dbos).pause();

    // Every minute at second 0: "0 * * * * *" (6-field cron)
    dbos.createSchedule(
        "backfill-correct", workflowName(), className(), "0 * * * * *", null, false, null, null);

    // Start at 10:00:30, end at 10:03:30
    // Should get: 10:01:00, 10:02:00, 10:03:00 (3 executions)
    var start = Instant.parse("2024-01-01T10:00:30Z");
    var end = Instant.parse("2024-01-01T10:03:30Z");

    impl.reset();
    var handles = dbos.backfillSchedule("backfill-correct", start, end);

    assertEquals(3, handles.size());

    for (var h : handles) {
      h.getResult();
    }

    assertEquals(3, impl.counter);
    var times = impl.allScheduledTimes.stream().sorted().toList();
    assertEquals(Instant.parse("2024-01-01T10:01:00Z"), times.get(0));
    assertEquals(Instant.parse("2024-01-01T10:02:00Z"), times.get(1));
    assertEquals(Instant.parse("2024-01-01T10:03:00Z"), times.get(2));
  }

  @Test
  public void backfillScheduleHourly() throws Exception {
    var impl = registerAndLaunch();

    // Pause scheduler to prevent interference with backfill results
    DBOSTestAccess.getSchedulerService(dbos).pause();

    // Every hour at minute 0: "0 0 * * * *" (6-field cron, runs at top of each hour)
    dbos.createSchedule(
        "backfill-hourly", workflowName(), className(), "0 0 * * * *", null, false, null, null);

    // Start at 09:30, end at 14:30
    // Should get: 10:00, 11:00, 12:00, 13:00, 14:00 (5 executions)
    var start = Instant.parse("2024-01-01T09:30:00Z");
    var end = Instant.parse("2024-01-01T14:30:00Z");

    impl.reset();
    var handles = dbos.backfillSchedule("backfill-hourly", start, end);

    assertEquals(5, handles.size());

    for (var h : handles) {
      h.getResult();
    }

    assertEquals(5, impl.counter);
    var times = impl.allScheduledTimes.stream().sorted().toList();
    assertEquals(Instant.parse("2024-01-01T10:00:00Z"), times.get(0));
    assertEquals(Instant.parse("2024-01-01T11:00:00Z"), times.get(1));
    assertEquals(Instant.parse("2024-01-01T12:00:00Z"), times.get(2));
    assertEquals(Instant.parse("2024-01-01T13:00:00Z"), times.get(3));
    assertEquals(Instant.parse("2024-01-01T14:00:00Z"), times.get(4));
  }

  @Test
  public void backfillScheduleDaily() throws Exception {
    var impl = registerAndLaunch();

    // Pause scheduler to prevent interference with backfill results
    DBOSTestAccess.getSchedulerService(dbos).pause();

    // Every day at midnight: "0 0 0 * * *" (6-field cron)
    dbos.createSchedule(
        "backfill-daily", workflowName(), className(), "0 0 0 * * *", null, false, null, null);

    // Backfill a week
    var start = Instant.parse("2024-01-01T12:00:00Z");
    var end = Instant.parse("2024-01-08T12:00:00Z");

    impl.reset();
    var handles = dbos.backfillSchedule("backfill-daily", start, end);

    // Jan 2-8 inclusive = 7 days
    assertEquals(7, handles.size());

    for (var h : handles) {
      h.getResult();
    }

    assertEquals(7, impl.counter);

    // Verify all times are at midnight
    for (Instant time : impl.allScheduledTimes) {
      assertEquals(0, time.atZone(ZoneId.of("UTC")).getHour());
      assertEquals(0, time.atZone(ZoneId.of("UTC")).getMinute());
      assertEquals(0, time.atZone(ZoneId.of("UTC")).getSecond());
    }
  }

  @Test
  public void triggerScheduleExecutesWorkflow() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        "trigger-sched", workflowName(), className(), "0 0 0 * * *", null, false, null, null);

    impl.reset();
    var handle = dbos.triggerSchedule("trigger-sched");
    handle.getResult();

    assertEquals(1, impl.counter);
    assertNotNull(impl.lastScheduled);
  }

  @Test
  public void triggerScheduleWithContext() throws Exception {
    var impl = registerAndLaunch();

    // Use a cron that won't fire during the test (Jan 1st only)
    dbos.createSchedule(
        "trigger-ctx",
        workflowName(),
        className(),
        "0 0 0 1 1 *",
        "test-context",
        false,
        null,
        null);

    impl.reset();
    var handle = dbos.triggerSchedule("trigger-ctx");
    handle.getResult();

    // At least 1 execution from trigger
    assertTrue(impl.counter >= 1, "Expected at least 1, got " + impl.counter);
    assertEquals("test-context", impl.lastContext);
  }

  // ── End-to-end ────────────────────────────────────────────────────────────

  @Test
  public void scheduleRunsAfterPolling() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        "run-sched", "latchedRun", className(), "0/1 * * * * *", null, false, null, null);

    // Verify schedule was created and is active
    var schedule = dbos.getSchedule("run-sched");
    assertTrue(schedule.isPresent(), "Schedule should be created");
    assertEquals(ScheduleStatus.ACTIVE, schedule.get().status());

    // latch initialized with 3 count, with each execution counting down once.
    // Wait for all 3 counts to be released, which indicates the workflow ran at least 3 times
    // (scheduler should run it every second).
    assertTrue(
        impl.latch.await(5, TimeUnit.SECONDS),
        "Expected latch to count down to zero within 5 seconds");
  }
}
