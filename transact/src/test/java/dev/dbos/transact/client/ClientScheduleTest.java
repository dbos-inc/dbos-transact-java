package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientScheduleTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  ClientScheduleServiceImpl serviceImpl;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig().withSchedulerPollingInterval(Duration.ofSeconds(1));
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    serviceImpl = new ClientScheduleServiceImpl();
    dbos.registerProxy(ClientScheduleService.class, serviceImpl);

    dbos.launch();
  }

  private static String workflowName() {
    return "scheduledRun";
  }

  private static String className() {
    return ClientScheduleServiceImpl.class.getName();
  }

  // ── createSchedule ────────────────────────────────────────────────────────

  @Test
  public void clientCreateAndGetSchedule() {
    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("my-sched", workflowName(), className(), "0/5 * * * * *"));

      var s = client.getSchedule("my-sched").orElseThrow();
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
  }

  @Test
  public void clientCreateScheduleDuplicate() {
    try (var client = pgContainer.dbosClient()) {
      var dupSched =
          new WorkflowSchedule("dup-sched", workflowName(), className(), "0/5 * * * * *");

      client.createSchedule(dupSched);
      assertThrows(RuntimeException.class, () -> client.createSchedule(dupSched));
    }
  }

  @Test
  public void clientCreateScheduleInvalidCron() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          RuntimeException.class,
          () ->
              client.createSchedule(
                  new WorkflowSchedule("bad-cron", workflowName(), className(), "not-a-cron")));
    }
  }

  // ── getSchedule ───────────────────────────────────────────────────────────

  @Test
  public void clientGetScheduleNotFound() {
    try (var client = pgContainer.dbosClient()) {
      assertTrue(client.getSchedule("nonexistent").isEmpty());
    }
  }

  // ── deleteSchedule ────────────────────────────────────────────────────────

  @Test
  public void clientDeleteSchedule() {
    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("del-sched", workflowName(), className(), "0/5 * * * * *"));
      assertTrue(client.getSchedule("del-sched").isPresent());

      client.deleteSchedule("del-sched");
      assertTrue(client.getSchedule("del-sched").isEmpty());
    }
  }

  @Test
  public void clientDeleteScheduleNotFound() {
    try (var client = pgContainer.dbosClient()) {
      assertDoesNotThrow(() -> client.deleteSchedule("nonexistent"));
    }
  }

  // ── pauseSchedule / resumeSchedule ────────────────────────────────────────

  @Test
  public void clientPauseAndResumeSchedule() {
    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("pause-sched", workflowName(), className(), "0/5 * * * * *"));

      client.pauseSchedule("pause-sched");
      assertEquals(ScheduleStatus.PAUSED, client.getSchedule("pause-sched").orElseThrow().status());

      client.resumeSchedule("pause-sched");
      assertEquals(ScheduleStatus.ACTIVE, client.getSchedule("pause-sched").orElseThrow().status());
    }
  }

  // ── listSchedules ───────────────────────────────────────────────────────

  @Test
  public void clientListSchedules() {
    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("alpha-1", workflowName(), className(), "0/5 * * * * *"));
      client.createSchedule(
          new WorkflowSchedule("alpha-2", workflowName(), className(), "0/5 * * * * *"));
      client.createSchedule(
          new WorkflowSchedule("beta-1", workflowName(), className(), "0/5 * * * * *"));
      client.pauseSchedule("beta-1");

      assertEquals(3, client.listSchedules(null, null, null).size());

      assertEquals(2, client.listSchedules(List.of(ScheduleStatus.ACTIVE), null, null).size());
      assertEquals(1, client.listSchedules(List.of(ScheduleStatus.PAUSED), null, null).size());

      assertEquals(3, client.listSchedules(null, List.of(workflowName()), null).size());
      assertEquals(0, client.listSchedules(null, List.of("unknownWorkflow"), null).size());

      assertEquals(2, client.listSchedules(null, null, List.of("alpha-")).size());
      assertEquals(1, client.listSchedules(null, null, List.of("beta-")).size());
      assertEquals(3, client.listSchedules(null, null, List.of("alpha-", "beta-")).size());
    }
  }

  // ── applySchedules ────────────────────────────────────────────────────────

  @Test
  public void clientApplySchedulesList() {
    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("apply-1", workflowName(), className(), "0/5 * * * * *"));
      var originalId = client.getSchedule("apply-1").orElseThrow().id();

      client.applySchedules(
          List.of(
              new WorkflowSchedule("apply-1", workflowName(), className(), "0/10 * * * * *"),
              new WorkflowSchedule("apply-2", workflowName(), className(), "0/5 * * * * *")));

      assertEquals(2, client.listSchedules(null, null, null).size());
      var replaced = client.getSchedule("apply-1").orElseThrow();
      assertEquals("0/10 * * * * *", replaced.cron());
      assertNotNull(replaced.id());
      assertNotEquals(originalId, replaced.id()); // new UUID assigned
      assertNull(replaced.lastFiredAt());
      var created = client.getSchedule("apply-2").orElseThrow();
      assertNotNull(created.id());
      assertNull(created.lastFiredAt());
    }
  }

  @Test
  public void clientApplySchedulesVarargs() {
    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("vargs-1", workflowName(), className(), "0/5 * * * * *"));

      client.applySchedules(
          new WorkflowSchedule("vargs-1", workflowName(), className(), "0/10 * * * * *"),
          new WorkflowSchedule("vargs-2", workflowName(), className(), "0/5 * * * * *"));

      assertEquals(2, client.listSchedules(null, null, null).size());
      assertEquals("0/10 * * * * *", client.getSchedule("vargs-1").orElseThrow().cron());
      assertTrue(client.getSchedule("vargs-2").isPresent());
    }
  }

  @Test
  public void clientApplySchedulesVarargsSingle() {
    try (var client = pgContainer.dbosClient()) {
      client.applySchedules(
          new WorkflowSchedule("single-varg", workflowName(), className(), "0/5 * * * * *"));

      assertEquals(1, client.listSchedules(null, null, null).size());
      assertEquals(ScheduleStatus.ACTIVE, client.getSchedule("single-varg").orElseThrow().status());
    }
  }

  @Test
  public void clientApplySchedulesVarargsEmpty() {
    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("pre-existing", workflowName(), className(), "0/5 * * * * *"));

      client.applySchedules(); // no-op — does not delete pre-existing schedules

      assertEquals(1, client.listSchedules(null, null, null).size());
    }
  }

  @Test
  public void clientApplySchedulesAlwaysCreatesActive() {
    try (var client = pgContainer.dbosClient()) {
      client.applySchedules(
          new WorkflowSchedule("apply-paused", workflowName(), className(), "0/5 * * * * *")
              .withStatus(ScheduleStatus.PAUSED));

      assertEquals(
          ScheduleStatus.ACTIVE, client.getSchedule("apply-paused").orElseThrow().status());
    }
  }

  @Test
  public void clientApplySchedulesInvalidCron() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          RuntimeException.class,
          () ->
              client.applySchedules(
                  new WorkflowSchedule("bad-cron", workflowName(), className(), "not-a-cron")));
    }
  }

  @Test
  public void clientApplySchedulesNullScheduleName() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          NullPointerException.class,
          () ->
              client.applySchedules(
                  new WorkflowSchedule(null, workflowName(), className(), "0/5 * * * * *")));
    }
  }

  @Test
  public void clientApplySchedulesNullWorkflowName() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          NullPointerException.class,
          () ->
              client.applySchedules(
                  new WorkflowSchedule("null-wf", null, className(), "0/5 * * * * *")));
    }
  }

  @Test
  public void clientApplySchedulesNullCron() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          NullPointerException.class,
          () ->
              client.applySchedules(
                  new WorkflowSchedule("null-cron", workflowName(), className(), null)));
    }
  }

  // ── triggerSchedule ───────────────────────────────────────────────────────

  @Test
  public void clientTriggerSchedule() throws Exception {
    try (var client = pgContainer.dbosClient()) {
      // Use a cron that won't fire during the test (Jan 1st only)
      client.createSchedule(
          new WorkflowSchedule("trigger-sched", workflowName(), className(), "0 0 0 1 1 *"));

      serviceImpl.reset();
      var handle = client.triggerSchedule("trigger-sched");
      handle.getResult();

      // At least 1 execution from trigger
      assertTrue(serviceImpl.counter >= 1, "Expected at least 1, got " + serviceImpl.counter);
      assertNotNull(serviceImpl.lastScheduled);
    }
  }

  @Test
  public void clientTriggerSchedulePassesContext() throws Exception {
    try (var client = pgContainer.dbosClient()) {
      // Use a cron that won't fire during the test (Jan 1st only)
      client.createSchedule(
          new WorkflowSchedule("ctx-sched", workflowName(), className(), "0 0 0 1 1 *")
              .withContext("my-context"));

      serviceImpl.reset();
      client.triggerSchedule("ctx-sched").getResult();
      // At least 1 execution from trigger
      assertTrue(serviceImpl.counter >= 1, "Expected at least 1, got " + serviceImpl.counter);
      assertEquals("my-context", serviceImpl.lastContext);
    }
  }

  @Test
  public void clientTriggerScheduleNotFound() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(IllegalStateException.class, () -> client.triggerSchedule("no-such-sched"));
    }
  }

  // ── backfillSchedule ─────────────────────────────────────────────────────

  @Test
  public void clientBackfillSchedule() throws Exception {
    // Pause scheduler to prevent interference with backfill results
    DBOSTestAccess.getSchedulerService(dbos).pause();

    try (var client = pgContainer.dbosClient()) {
      // Use a cron that won't fire during the test (every minute)
      client.createSchedule(
          new WorkflowSchedule("backfill-sched", workflowName(), className(), "0 * * * * *"));

      // Backfill for times that won't be picked up by the scheduler
      var start = Instant.parse("2024-01-01T10:00:30Z");
      var end = Instant.parse("2024-01-01T10:03:30Z");

      serviceImpl.reset();
      var handles = client.backfillSchedule("backfill-sched", start, end);

      assertEquals(3, handles.size());
      for (var h : handles) {
        h.getResult();
      }
      // Backfill creates exactly 3
      assertEquals(3, serviceImpl.counter);
    }
  }

  @Test
  public void clientBackfillScheduleEmptyWindow() {
    // Pause scheduler to prevent interference with backfill results
    DBOSTestAccess.getSchedulerService(dbos).pause();

    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("backfill-empty", workflowName(), className(), "0/1 * * * * *"));

      var t = Instant.now().truncatedTo(ChronoUnit.SECONDS);
      var handles = client.backfillSchedule("backfill-empty", t, t);
      assertEquals(0, handles.size());
    }
  }

  @Test
  public void clientBackfillScheduleNotFound() {
    try (var client = pgContainer.dbosClient()) {
      var t = Instant.now();
      assertThrows(
          IllegalStateException.class,
          () -> client.backfillSchedule("no-such-sched", t, t.plusSeconds(10)));
    }
  }

  // ── Cross-API consistency ────────────────────────────────────────────────

  @Test
  public void clientScheduleCrudConsistency() {
    try (var client = pgContainer.dbosClient()) {
      client.createSchedule(
          new WorkflowSchedule("cross-sched", workflowName(), className(), "0/5 * * * * *"));

      var dbosSchedule = dbos.getSchedule("cross-sched").orElseThrow();
      var clientSchedule = client.getSchedule("cross-sched").orElseThrow();

      assertEquals(dbosSchedule.scheduleName(), clientSchedule.scheduleName());
      assertEquals(dbosSchedule.workflowName(), clientSchedule.workflowName());
      assertEquals(dbosSchedule.cron(), clientSchedule.cron());
      assertEquals(dbosSchedule.status(), clientSchedule.status());

      client.pauseSchedule("cross-sched");
      assertEquals(dbos.getSchedule("cross-sched").orElseThrow().status(), ScheduleStatus.PAUSED);
      assertEquals(client.getSchedule("cross-sched").orElseThrow().status(), ScheduleStatus.PAUSED);

      client.resumeSchedule("cross-sched");
      assertEquals(dbos.getSchedule("cross-sched").orElseThrow().status(), ScheduleStatus.ACTIVE);
      assertEquals(client.getSchedule("cross-sched").orElseThrow().status(), ScheduleStatus.ACTIVE);

      client.deleteSchedule("cross-sched");
      assertTrue(dbos.getSchedule("cross-sched").isEmpty());
      assertTrue(client.getSchedule("cross-sched").isEmpty());
    }
  }
}

interface ClientScheduleService {
  void scheduledRun(Instant scheduled, Object context);
}

class ClientScheduleServiceImpl implements ClientScheduleService {

  volatile int counter = 0;
  volatile Instant lastScheduled = null;
  volatile Object lastContext = null;
  final java.util.List<Instant> allScheduledTimes =
      new java.util.concurrent.CopyOnWriteArrayList<>();

  @Override
  @dev.dbos.transact.workflow.Workflow
  public void scheduledRun(Instant scheduled, Object context) {
    ++counter;
    lastScheduled = scheduled;
    lastContext = context;
    allScheduledTimes.add(scheduled);
  }

  void reset() {
    counter = 0;
    lastScheduled = null;
    lastContext = null;
    allScheduledTimes.clear();
  }
}
