package dev.dbos.transact.scheduled;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.execution.SchedulerService;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

/** A scheduled workflow's ID spells its tick one way, whichever path computes it. */
class ScheduledWorkflowIdTest {

  private static String id(String time, String zone) {
    return SchedulerService.scheduledWorkflowId("s", Instant.parse(time).atZone(ZoneId.of(zone)));
  }

  @Test
  void zeroSecondsAreLeftOut() {
    assertEquals("sched-s-2026-10-05T12:00Z", id("2026-10-05T12:00:00Z", "UTC"));
  }

  @Test
  void subMinuteTicksKeepTheirSeconds() {
    assertEquals("sched-s-2026-10-05T12:00:30Z", id("2026-10-05T12:00:30Z", "UTC"));
  }

  @Test
  void aRegionZoneIsSpelledAsItsOffset() {
    assertEquals("sched-s-2026-10-05T08:00-04:00", id("2026-10-05T12:00:00Z", "America/New_York"));
    assertEquals("sched-s-2026-01-05T07:00-05:00", id("2026-01-05T12:00:00Z", "America/New_York"));
  }

  @Test
  void aZeroOffsetIsSpelledZ() {
    var tick = ZonedDateTime.of(2026, 10, 5, 12, 0, 0, 0, ZoneOffset.ofHours(0));
    assertEquals("sched-s-2026-10-05T12:00Z", SchedulerService.scheduledWorkflowId("s", tick));
  }

  @Test
  void aScheduleWithoutATimeZoneIsReadInUtc() {
    assertEquals(ZoneOffset.UTC, SchedulerService.cronZone(null));
    var newYork = ZoneId.of("America/New_York");
    assertEquals(newYork, SchedulerService.cronZone(newYork));
  }
}
