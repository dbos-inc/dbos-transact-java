package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

/**
 * Covers the constructors that omit the trailing application name, which exist so callers that
 * predate system database sharing keep compiling. They delegate positionally, so each component is
 * checked to catch a mis-ordered delegation, which would otherwise compile cleanly.
 */
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
}
