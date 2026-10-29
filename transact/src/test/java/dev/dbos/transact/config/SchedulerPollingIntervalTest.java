package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * The scheduler polls at a fixed rate in whole milliseconds, so an interval that truncates to zero
 * is refused where the configuration is built, with an error naming the setting, rather than by the
 * executor service partway through launch.
 */
public class SchedulerPollingIntervalTest {

  @Test
  public void anIntervalUnderOneMillisecondIsRefused() {
    for (var interval : List.of(Duration.ZERO, Duration.ofSeconds(-1), Duration.ofNanos(500))) {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> DBOSConfig.defaults("app").withSchedulerPollingInterval(interval),
              interval.toString());
      assertTrue(ex.getMessage().contains("schedulerPollingInterval"), ex.getMessage());
    }
  }

  @Test
  public void oneMillisecondAndUnsetAreAccepted() {
    assertEquals(
        Duration.ofMillis(1),
        DBOSConfig.defaults("app")
            .withSchedulerPollingInterval(Duration.ofMillis(1))
            .schedulerPollingInterval());
    assertNull(
        DBOSConfig.defaults("app").withSchedulerPollingInterval(null).schedulerPollingInterval());
  }
}
