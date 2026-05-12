package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    dbos.registerProxy(ScheduledWorkflowService.class, impl);
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
        new WorkflowSchedule("my-sched", workflowName(), className(), "0/5 * * * * *"));

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
    dbos.registerProxy(ScheduledWorkflowService.class, new ScheduledWorkflowImpl());
    dbos.launch();

    dbos.createSchedule(
        new WorkflowSchedule("full-sched", workflowName(), className(), "0 0 * * * *")
            .withContext("{\"key\":\"val\"}")
            .withAutomaticBackfill(true)
            .withCronTimezone(ZoneId.of("America/New_York"))
            .withQueueName("sched-q"));

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
        new WorkflowSchedule("dup-sched", workflowName(), className(), "0/5 * * * * *"));
    assertThrows(
        RuntimeException.class,
        () ->
            dbos.createSchedule(
                new WorkflowSchedule("dup-sched", workflowName(), className(), "0/5 * * * * *")));
  }

  @Test
  public void createScheduleInvalidCron() {
    registerAndLaunch();
    assertThrows(
        RuntimeException.class,
        () ->
            dbos.createSchedule(
                new WorkflowSchedule("bad-cron", workflowName(), className(), "not-a-cron")));
  }

  @Test
  public void createScheduleUnknownWorkflow() {
    registerAndLaunch();
    assertThrows(
        IllegalStateException.class,
        () ->
            dbos.createSchedule(
                new WorkflowSchedule("bad-wf", "noSuchMethod", className(), "0/5 * * * * *")));
  }

  @Test
  public void createScheduleUnknownQueue() {
    registerAndLaunch();
    assertThrows(
        IllegalStateException.class,
        () ->
            dbos.createSchedule(
                new WorkflowSchedule("bad-q", workflowName(), className(), "0/5 * * * * *")
                    .withQueueName("no-such-queue")));
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
        new WorkflowSchedule("del-sched", workflowName(), className(), "0/5 * * * * *"));
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
        new WorkflowSchedule("pause-sched", workflowName(), className(), "0/5 * * * * *"));

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
        new WorkflowSchedule("alpha-1", workflowName(), className(), "0/5 * * * * *"));
    dbos.createSchedule(
        new WorkflowSchedule("alpha-2", workflowName(), className(), "0/5 * * * * *"));
    dbos.createSchedule(
        new WorkflowSchedule("beta-1", workflowName(), className(), "0/5 * * * * *"));
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
        new WorkflowSchedule("apply-1", workflowName(), className(), "0/5 * * * * *"));
    var originalId = dbos.getSchedule("apply-1").orElseThrow().id();
    assertEquals(1, dbos.listSchedules(null, null, null).size());

    // apply-1 is replaced (new cron) and apply-2 is created — applySchedules upserts, it does
    // not delete schedules absent from the list.
    dbos.applySchedules(
        List.of(
            new WorkflowSchedule("apply-1", workflowName(), className(), "0/10 * * * * *"),
            new WorkflowSchedule("apply-2", workflowName(), className(), "0/5 * * * * *")));

    assertEquals(2, dbos.listSchedules(null, null, null).size());
    var replaced = dbos.getSchedule("apply-1").orElseThrow();
    assertEquals("0/10 * * * * *", replaced.cron());
    assertNotNull(replaced.id());
    assertNotEquals(originalId, replaced.id()); // new UUID assigned
    assertNull(replaced.lastFiredAt());
    var created = dbos.getSchedule("apply-2").orElseThrow();
    assertNotNull(created.id());
    assertNull(created.lastFiredAt());
  }

  @Test
  public void applySchedulesAlwaysCreatesActive() {
    // applySchedules ignores the status field in the input — it always creates ACTIVE
    registerAndLaunch();

    dbos.applySchedules(
        List.of(
            new WorkflowSchedule("apply-paused", workflowName(), className(), "0/5 * * * * *")
                .withStatus(ScheduleStatus.PAUSED)));

    assertEquals(ScheduleStatus.ACTIVE, dbos.getSchedule("apply-paused").orElseThrow().status());
  }

  @Test
  public void applySchedulesVarargsCreatesAndReplaces() {
    registerAndLaunch();

    dbos.createSchedule(
        new WorkflowSchedule("vargs-1", workflowName(), className(), "0/5 * * * * *"));

    dbos.applySchedules(
        new WorkflowSchedule("vargs-1", workflowName(), className(), "0/10 * * * * *"),
        new WorkflowSchedule("vargs-2", workflowName(), className(), "0/5 * * * * *"));

    assertEquals(2, dbos.listSchedules(null, null, null).size());
    assertEquals("0/10 * * * * *", dbos.getSchedule("vargs-1").orElseThrow().cron());
    assertTrue(dbos.getSchedule("vargs-2").isPresent());
  }

  @Test
  public void applySchedulesVarargsSingle() {
    registerAndLaunch();

    dbos.applySchedules(
        new WorkflowSchedule("single-varg", workflowName(), className(), "0/5 * * * * *"));

    assertEquals(1, dbos.listSchedules(null, null, null).size());
    assertEquals(ScheduleStatus.ACTIVE, dbos.getSchedule("single-varg").orElseThrow().status());
  }

  @Test
  public void applySchedulesVarargsEmpty() {
    registerAndLaunch();

    dbos.createSchedule(
        new WorkflowSchedule("pre-existing", workflowName(), className(), "0/5 * * * * *"));

    dbos.applySchedules(); // no-op — does not delete pre-existing schedules

    assertEquals(1, dbos.listSchedules(null, null, null).size());
  }

  @Test
  public void applySchedulesInvalidCron() {
    registerAndLaunch();
    assertThrows(
        RuntimeException.class,
        () ->
            dbos.applySchedules(
                new WorkflowSchedule("bad-cron", workflowName(), className(), "not-a-cron")));
  }

  @Test
  public void applySchedulesNullScheduleName() {
    registerAndLaunch();
    assertThrows(
        NullPointerException.class,
        () ->
            dbos.applySchedules(
                new WorkflowSchedule(null, workflowName(), className(), "0/5 * * * * *")));
  }

  @Test
  public void applySchedulesNullWorkflowName() {
    registerAndLaunch();
    assertThrows(
        RuntimeException.class,
        () ->
            dbos.applySchedules(
                new WorkflowSchedule("null-wf", null, className(), "0/5 * * * * *")));
  }

  @Test
  public void applySchedulesNullClassName() {
    registerAndLaunch();
    assertThrows(
        RuntimeException.class,
        () ->
            dbos.applySchedules(
                new WorkflowSchedule("null-class", workflowName(), null, "0/5 * * * * *")));
  }

  @Test
  public void applySchedulesNullCron() {
    registerAndLaunch();
    assertThrows(
        NullPointerException.class,
        () ->
            dbos.applySchedules(
                new WorkflowSchedule("null-cron", workflowName(), className(), null)));
  }

  @Test
  public void applySchedulesUnknownWorkflow() {
    registerAndLaunch();
    assertThrows(
        IllegalStateException.class,
        () ->
            dbos.applySchedules(
                new WorkflowSchedule("bad-wf", "noSuchMethod", className(), "0/5 * * * * *")));
  }

  @Test
  public void applySchedulesUnknownQueue() {
    registerAndLaunch();
    assertThrows(
        IllegalStateException.class,
        () ->
            dbos.applySchedules(
                new WorkflowSchedule("bad-q", workflowName(), className(), "0/5 * * * * *")
                    .withQueueName("no-such-queue")));
  }

  // ── triggerSchedule ───────────────────────────────────────────────────────

  @Test
  public void triggerSchedule() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        new WorkflowSchedule(
            "trigger-sched", workflowName(), className(), "0 0 0 1 1 *")); // Jan 1st only

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
        new WorkflowSchedule("ctx-sched", workflowName(), className(), "0 0 0 1 1 *")
            .withContext("my-context"));

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
        new WorkflowSchedule("backfill-sched", workflowName(), className(), "0 * * * * *"));

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
        new WorkflowSchedule("backfill-empty", workflowName(), className(), "0/1 * * * * *"));

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
        new WorkflowSchedule("backfill-correct", workflowName(), className(), "0 * * * * *"));

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
        new WorkflowSchedule("backfill-hourly", workflowName(), className(), "0 0 * * * *"));

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
        new WorkflowSchedule("backfill-daily", workflowName(), className(), "0 0 0 * * *"));

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
        new WorkflowSchedule("trigger-sched", workflowName(), className(), "0 0 0 * * *"));

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
        new WorkflowSchedule("trigger-ctx", workflowName(), className(), "0 0 0 1 1 *")
            .withContext("test-context"));

    impl.reset();
    var handle = dbos.triggerSchedule("trigger-ctx");
    handle.getResult();

    // At least 1 execution from trigger
    assertTrue(impl.counter >= 1, "Expected at least 1, got " + impl.counter);
    assertEquals("test-context", impl.lastContext);
  }

  // ── Workflow ID format ────────────────────────────────────────────────────

  @Test
  public void workflowIdUsesOffsetDateTimeFormat() throws Exception {
    // Scheduler-generated IDs must use OffsetDateTime (e.g. 2026-04-20T10:15:30+00:00),
    // not ZonedDateTime which appends zone name in brackets (e.g. [UTC]).
    var impl = registerAndLaunch();
    dbos.createSchedule(
        new WorkflowSchedule("id-fmt-sched", "latchedRun", className(), "0/1 * * * * *"));

    assertTrue(impl.latch.await(15, TimeUnit.SECONDS));

    var prefix = "sched-id-fmt-sched-";
    var workflows = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIdPrefix(prefix));
    assertFalse(workflows.isEmpty());
    for (var wf : workflows) {
      var id = wf.workflowId();
      assertFalse(id.contains("["), "Workflow ID must not contain zone name brackets: " + id);
      assertDoesNotThrow(
          () -> OffsetDateTime.parse(id.substring(prefix.length())),
          "Date suffix must be a valid OffsetDateTime: " + id);
    }
  }

  @Test
  public void workflowIdIncludesTimezoneOffsetNotZoneName() throws Exception {
    // When cronTimezone is a named zone (e.g. America/New_York), the ID must include the
    // numeric offset (e.g. -04:00) but not the zone name in brackets.
    var impl = registerAndLaunch();
    dbos.createSchedule(
        new WorkflowSchedule("tz-sched", "latchedRun", className(), "0/1 * * * * *")
            .withCronTimezone(ZoneId.of("America/New_York")));

    assertTrue(impl.latch.await(15, TimeUnit.SECONDS));

    var prefix = "sched-tz-sched-";
    var workflows = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIdPrefix(prefix));
    assertFalse(workflows.isEmpty());
    for (var wf : workflows) {
      var id = wf.workflowId();
      assertFalse(id.contains("["), "Workflow ID must not contain zone name brackets: " + id);
      assertFalse(id.contains("America"), "Workflow ID must not contain zone name: " + id);
      assertDoesNotThrow(
          () -> OffsetDateTime.parse(id.substring(prefix.length())),
          "Date suffix must be a valid OffsetDateTime: " + id);
    }
  }

  // ── End-to-end ────────────────────────────────────────────────────────────

  @Test
  public void scheduleRunsAfterPolling() throws Exception {
    var impl = registerAndLaunch();

    dbos.createSchedule(
        new WorkflowSchedule("run-sched", "latchedRun", className(), "0/1 * * * * *"));

    // Verify schedule was created and is active
    var schedule = dbos.getSchedule("run-sched");
    assertTrue(schedule.isPresent(), "Schedule should be created");
    assertEquals(ScheduleStatus.ACTIVE, schedule.get().status());

    // latch initialized with 3 count, with each execution counting down once.
    // Wait for all 3 counts to be released, which indicates the workflow ran at least 3 times
    // (scheduler should run it every second).
    assertTrue(
        impl.latch.await(15, TimeUnit.SECONDS),
        "Expected latch to count down to zero within 15 seconds");
  }
}
