package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Covers the constructors that omit the application name, which exist so callers that predate
 * system database sharing keep compiling. They delegate positionally, so each is checked against
 * the canonical constructor to catch a mis-ordered delegation, which would otherwise compile
 * cleanly. Distinct values per component are what make a swapped pair visible.
 */
// Builds Queue values as fixtures or mock stubs, never to register one: the type stays,
// only authoring a Queue by hand is deprecated.
@SuppressWarnings("removal")
class OmittedApplicationNameTest {

  @Test
  void scheduleConstructedWithoutAnApplicationNameIsUnclaimed() {
    var lastFiredAt = Instant.ofEpochSecond(1_700_000_000L);
    var schedule =
        new WorkflowSchedule(
            "schedule-id",
            "schedule-name",
            "workflow-name",
            "class-name",
            "* * * * * *",
            ScheduleStatus.PAUSED,
            "context",
            lastFiredAt,
            true,
            ZoneId.of("America/New_York"),
            "queue-name");

    assertNull(schedule.applicationName());
    assertEquals("schedule-id", schedule.id());
    assertEquals("schedule-name", schedule.scheduleName());
    assertEquals("workflow-name", schedule.workflowName());
    assertEquals("class-name", schedule.className());
    assertEquals("* * * * * *", schedule.cron());
    assertEquals(ScheduleStatus.PAUSED, schedule.status());
    assertEquals("context", schedule.context());
    assertEquals(lastFiredAt, schedule.lastFiredAt());
    assertEquals(true, schedule.automaticBackfill());
    assertEquals(ZoneId.of("America/New_York"), schedule.cronTimezone());
    assertEquals("queue-name", schedule.queueName());
  }

  @Test
  void queueConstructedWithoutAnApplicationNameIsUnclaimed() {
    var rateLimit = new Queue.RateLimit(5, Duration.ofSeconds(10));
    var queue = new Queue("queue-name", 3, 2, true, true, rateLimit, Duration.ofSeconds(7));

    assertNull(queue.applicationName());
    assertEquals("queue-name", queue.name());
    assertEquals(3, queue.concurrency());
    assertEquals(2, queue.workerConcurrency());
    assertEquals(true, queue.priorityEnabled());
    assertEquals(true, queue.partitioningEnabled());
    assertEquals(rateLimit, queue.rateLimit());
    assertEquals(Duration.ofSeconds(7), queue.pollingInterval());
  }

  @Test
  void listWorkflowsInputConstructedWithoutAnApplicationNameFiltersOnNone() {
    var attributes = Map.<String, Object>of("key", "value");
    var actual =
        new ListWorkflowsInput(
            List.of("workflow-ids"),
            List.of(WorkflowState.SUCCESS),
            Instant.ofEpochSecond(1),
            Instant.ofEpochSecond(2),
            List.of("workflow-name"),
            "class-name",
            "instance-name",
            List.of("application-version"),
            List.of("authenticated-user"),
            10,
            20,
            true,
            List.of("workflow-id-prefix"),
            true,
            false,
            List.of("queue-name"),
            true,
            List.of("executor-ids"),
            List.of("forked-from"),
            List.of("parent-workflow-id"),
            false,
            true,
            true,
            attributes,
            Instant.ofEpochSecond(3),
            Instant.ofEpochSecond(4),
            Instant.ofEpochSecond(5),
            Instant.ofEpochSecond(6),
            List.of("schedule-name"));

    var expected =
        new ListWorkflowsInput(
            List.of("workflow-ids"),
            List.of(WorkflowState.SUCCESS),
            Instant.ofEpochSecond(1),
            Instant.ofEpochSecond(2),
            List.of("workflow-name"),
            "class-name",
            "instance-name",
            List.of("application-version"),
            List.of("authenticated-user"),
            10,
            20,
            true,
            List.of("workflow-id-prefix"),
            true,
            false,
            List.of("queue-name"),
            true,
            List.of("executor-ids"),
            List.of("forked-from"),
            List.of("parent-workflow-id"),
            false,
            true,
            true,
            attributes,
            Instant.ofEpochSecond(3),
            Instant.ofEpochSecond(4),
            Instant.ofEpochSecond(5),
            Instant.ofEpochSecond(6),
            List.of("schedule-name"),
            null);

    assertNull(actual.applicationName());
    assertEquals(expected, actual);
  }
}
