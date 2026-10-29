package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * The scheduler polls at a fixed rate in whole milliseconds, so an interval that truncates to zero,
 * or one too long for a long count of milliseconds, is refused where the configuration is built,
 * with an error naming the setting.
 */
public class SchedulerPollingIntervalTest {

  @Test
  public void anIntervalUnderOneMillisecondIsRefused() {
    for (var interval :
        List.of(
            Duration.ZERO,
            Duration.ofSeconds(-1),
            Duration.ofNanos(500),
            Duration.ofNanos(999_999))) {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> DBOSConfig.defaults("app").withSchedulerPollingInterval(interval),
              interval.toString());
      assertTrue(ex.getMessage().contains("schedulerPollingInterval"), ex.getMessage());
      assertTrue(ex.getMessage().contains("at least 1ms"), ex.getMessage());
    }
  }

  @Test
  public void anIntervalTooLongForMillisecondsIsRefused() {
    // Meant as "effectively never", but the scheduler would overflow converting it to millis.
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DBOSConfig.defaults("app")
                    .withSchedulerPollingInterval(ChronoUnit.FOREVER.getDuration()));
    assertTrue(ex.getMessage().contains("schedulerPollingInterval"), ex.getMessage());
    assertTrue(ex.getMessage().contains("must not exceed"), ex.getMessage());
  }

  @Test
  public void everyIntervalInRangeAndUnsetAreAccepted() {
    for (var interval :
        List.of(
            Duration.ofMillis(1),
            Duration.ofNanos(1_500_000),
            DBOSConfig.DEFAULT_SCHEDULER_POLLING_INTERVAL,
            DBOSConfig.MAX_SCHEDULER_POLLING_INTERVAL)) {
      assertEquals(
          interval,
          DBOSConfig.defaults("app")
              .withSchedulerPollingInterval(interval)
              .schedulerPollingInterval());
    }
    assertNull(
        DBOSConfig.defaults("app").withSchedulerPollingInterval(null).schedulerPollingInterval());
  }
}
