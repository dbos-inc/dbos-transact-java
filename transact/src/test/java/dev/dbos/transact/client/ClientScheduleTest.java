package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ScheduleStatus;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
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
    dbos.registerWorkflows(ClientScheduleService.class, serviceImpl);

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
          "my-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);

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
      client.createSchedule(
          "dup-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
      assertThrows(
          RuntimeException.class,
          () ->
              client.createSchedule(
                  "dup-sched",
                  workflowName(),
                  className(),
                  "0/5 * * * * *",
                  null,
                  false,
                  null,
                  null));
    }
  }

  @Test
  public void clientCreateScheduleInvalidCron() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          RuntimeException.class,
          () ->
              client.createSchedule(
                  "bad-cron", workflowName(), className(), "not-a-cron", null, false, null, null));
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
          "del-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
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
          "pause-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);

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
          "alpha-1", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
      client.createSchedule(
          "alpha-2", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
      client.createSchedule(
          "beta-1", workflowName(), className(), "0/5 * * * * *", null, false, null, null);
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

  // ── triggerSchedule ───────────────────────────────────────────────────────

  @Test
  public void clientTriggerSchedule() throws Exception {
    try (var client = pgContainer.dbosClient()) {
      // Use a cron that won't fire during the test (Jan 1st only)
      client.createSchedule(
          "trigger-sched", workflowName(), className(), "0 0 0 1 1 *", null, false, null, null);

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
          "ctx-sched", workflowName(), className(), "0 0 0 1 1 *", "my-context", false, null, null);

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
          "backfill-sched", workflowName(), className(), "0 * * * * *", null, false, null, null);

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
          "backfill-empty", workflowName(), className(), "0/1 * * * * *", null, false, null, null);

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
          "cross-sched", workflowName(), className(), "0/5 * * * * *", null, false, null, null);

      var dbosSchedule = dbos.findSchedule("cross-sched").orElseThrow();
      var clientSchedule = client.getSchedule("cross-sched").orElseThrow();

      assertEquals(dbosSchedule.scheduleName(), clientSchedule.scheduleName());
      assertEquals(dbosSchedule.workflowName(), clientSchedule.workflowName());
      assertEquals(dbosSchedule.cron(), clientSchedule.cron());
      assertEquals(dbosSchedule.status(), clientSchedule.status());

      client.pauseSchedule("cross-sched");
      assertEquals(dbos.findSchedule("cross-sched").orElseThrow().status(), ScheduleStatus.PAUSED);
      assertEquals(client.getSchedule("cross-sched").orElseThrow().status(), ScheduleStatus.PAUSED);

      client.resumeSchedule("cross-sched");
      assertEquals(dbos.findSchedule("cross-sched").orElseThrow().status(), ScheduleStatus.ACTIVE);
      assertEquals(client.getSchedule("cross-sched").orElseThrow().status(), ScheduleStatus.ACTIVE);

      client.deleteSchedule("cross-sched");
      assertTrue(dbos.findSchedule("cross-sched").isEmpty());
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
