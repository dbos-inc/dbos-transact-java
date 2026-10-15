package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

// The JVM's default time zone is global state, so these tests run alone, on one thread.
/**
 * A schedule's ticks get the same IDs fired live and backfilled, on a JVM whose default time zone
 * is not UTC, so backfill over ticks the scheduler already fired enqueues nothing new.
 */
@Isolated
@Execution(ExecutionMode.SAME_THREAD)
class ScheduleTimeZoneTest {

  // Neither UTC nor the region the second test schedules in.
  private static final TimeZone HOST_ZONE = TimeZone.getTimeZone("Asia/Kolkata");

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  private TimeZone savedZone;

  @BeforeEach
  void setHostZone() {
    savedZone = TimeZone.getDefault();
    TimeZone.setDefault(HOST_ZONE);
  }

  @AfterEach
  void restoreHostZone() {
    TimeZone.setDefault(savedZone);
  }

  @Test
  void aScheduleWithoutATimeZoneFiresInUtcAndBackfillsTheSameIds() throws Exception {
    var live = fireLiveThenBackfill("no-zone", null);
    for (var id : live) {
      assertTrue(id.endsWith("Z"), "not read in UTC: " + id);
    }
  }

  @Test
  void aRegionScheduleBackfillsTheSameIdsItFiredLive() throws Exception {
    var live = fireLiveThenBackfill("new-york", ZoneId.of("America/New_York"));
    for (var id : live) {
      assertTrue(id.endsWith("-04:00") || id.endsWith("-05:00"), "not New York's offset: " + id);
    }
  }

  // Lets the scheduler fire a per-second schedule a few times, stops it, and backfills over the
  // ticks it fired: backfill must compute the same IDs and enqueue nothing new. Returns the IDs.
  private Set<String> fireLiveThenBackfill(String name, @Nullable ZoneId cronTimezone)
      throws Exception {
    dbos = new DBOS(pgContainer.dbosConfig().withSchedulerPollingInterval(Duration.ofSeconds(1)));
    dbos.registerProxy(ScheduledWorkflowService.class, new ScheduledWorkflowImpl());
    dbos.launch();
    var schedule =
        new WorkflowSchedule(
            name, "scheduledRun", ScheduledWorkflowImpl.class.getName(), "* * * * * *");
    dbos.createSchedule(cronTimezone == null ? schedule : schedule.withCronTimezone(cronTimezone));

    var prefix = "sched-" + name + "-";
    var deadline = Instant.now().plusSeconds(15);
    while (ids(prefix).size() < 3) {
      assertTrue(Instant.now().isBefore(deadline), "the scheduler fired too few ticks");
      Thread.sleep(200);
    }
    DBOSTestAccess.getSchedulerService(dbos).pause();
    // A tick already being enqueued when the pause landed still lands.
    Thread.sleep(1_500);
    var live = ids(prefix);

    var ticks =
        live.stream()
            .map(id -> OffsetDateTime.parse(id.substring(prefix.length())).toInstant())
            .sorted()
            .toList();
    var backfilled =
        dbos.backfillSchedule(name, ticks.get(0).minusSeconds(1), ticks.get(ticks.size() - 1));

    assertEquals(
        live,
        backfilled.stream().map(h -> h.workflowId()).collect(Collectors.toSet()),
        "backfill computed other IDs for the ticks fired live");
    assertEquals(live, ids(prefix), "backfill enqueued ticks the scheduler already fired");
    return live;
  }

  private Set<String> ids(String prefix) {
    return dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIdPrefix(prefix)).stream()
        .map(WorkflowStatus::workflowId)
        .collect(Collectors.toSet());
  }
}
