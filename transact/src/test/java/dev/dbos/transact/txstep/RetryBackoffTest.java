package dev.dbos.transact.txstep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

public class RetryBackoffTest {

  @Test
  public void waitsGrowByHalfUpToTwoSeconds() {
    var backoff = new PostgresStepFactory.RetryBackoff();

    assertEquals(Duration.ofNanos(1_000_000), backoff.nextWait());
    assertEquals(Duration.ofNanos(1_500_000), backoff.nextWait());
    assertEquals(Duration.ofNanos(2_250_000), backoff.nextWait());
    assertEquals(Duration.ofNanos(3_375_000), backoff.nextWait());

    // 1 ms * 1.5^n first exceeds 2 s at n = 19, so the 20th wait is the cap.
    var previous = Duration.ofNanos(3_375_000);
    for (int i = 4; i < 19; i++) {
      var wait = backoff.nextWait();
      assertTrue(wait.compareTo(previous) > 0, "wait %d did not grow: %s".formatted(i, wait));
      assertTrue(wait.compareTo(Duration.ofSeconds(2)) < 0, "wait %d: %s".formatted(i, wait));
      previous = wait;
    }
    for (int i = 0; i < 5; i++) {
      assertEquals(Duration.ofSeconds(2), backoff.nextWait());
    }
  }

  @Test
  public void eachStepStartsAtOneMillisecond() {
    var first = new PostgresStepFactory.RetryBackoff();
    first.nextWait();
    first.nextWait();
    assertEquals(Duration.ofMillis(1), new PostgresStepFactory.RetryBackoff().nextWait());
  }
}
