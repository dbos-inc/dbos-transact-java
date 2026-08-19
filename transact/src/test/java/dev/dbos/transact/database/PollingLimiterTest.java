package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class PollingLimiterTest {

  @Test
  @DisplayName("A non-positive limit disables the limiter and every acquire succeeds")
  public void disabledLimiter() {
    var limiter = new PollingLimiter(0);
    assertFalse(limiter.isEnabled());
    assertEquals(Integer.MAX_VALUE, limiter.availablePermits());

    var held = new ArrayList<PollingLimiter.Permit>();
    for (int i = 0; i < 100; i++) {
      held.add(limiter.acquire());
    }
    assertEquals(Integer.MAX_VALUE, limiter.availablePermits());
    held.forEach(PollingLimiter.Permit::close);

    assertFalse(new PollingLimiter(-1).isEnabled());
  }

  @Test
  @DisplayName("Closing a permit returns it, and closing it twice does not")
  public void permitReleaseIsIdempotent() {
    var limiter = new PollingLimiter(2);
    var permit = limiter.acquire();
    assertEquals(1, limiter.availablePermits());

    permit.close();
    assertEquals(2, limiter.availablePermits());
    // A permit handed back twice would raise the cap for the life of the process.
    permit.close();
    assertEquals(2, limiter.availablePermits());
  }

  @Test
  @DisplayName("No more than the configured number of acquirers run at once")
  public void capsConcurrentAcquirers() throws Exception {
    var limit = 3;
    var threads = 12;
    var limiter = new PollingLimiter(limit);
    var inFlight = new AtomicInteger();
    var peak = new AtomicInteger();
    var start = new CountDownLatch(1);
    var done = new CountDownLatch(threads);

    for (int i = 0; i < threads; i++) {
      var t =
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int pass = 0; pass < 50; pass++) {
                    try (var permit = limiter.acquire()) {
                      var now = inFlight.incrementAndGet();
                      peak.accumulateAndGet(now, Math::max);
                      Thread.sleep(1);
                      inFlight.decrementAndGet();
                    }
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  done.countDown();
                }
              });
      t.setDaemon(true);
      t.start();
    }

    start.countDown();
    assertTrue(done.await(60, TimeUnit.SECONDS), "acquirers did not finish");
    assertEquals(0, inFlight.get());
    assertEquals(limit, limiter.availablePermits(), "permits were not all returned");
    assertTrue(
        peak.get() <= limit, "saw " + peak.get() + " concurrent acquirers, limit was " + limit);
    assertTrue(peak.get() > 1, "the limit was never actually contended; test proves nothing");
  }

  @Test
  @DisplayName("Acquiring does not consume the caller's interrupt")
  public void acquirePreservesInterruptStatus() {
    var limiter = new PollingLimiter(1);
    Thread.currentThread().interrupt();
    try {
      try (var permit = limiter.acquire()) {
        // The poll loop handles the interrupt at its next blocking point, not here.
        assertTrue(Thread.currentThread().isInterrupted());
      }
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  @DisplayName("The default limit is half the pool, and a configured limit is taken as given")
  public void resolvesTheDefaultFromThePoolSize() {
    try (var pooled = new HikariDataSource()) {
      pooled.setMaximumPoolSize(8);
      assertEquals(4, SystemDatabase.resolvePollingConcurrency(pooled, null));

      // A configured value wins over the pool, including one that turns the limiter off.
      assertEquals(1, SystemDatabase.resolvePollingConcurrency(pooled, 1));
      assertEquals(0, SystemDatabase.resolvePollingConcurrency(pooled, 0));
      assertEquals(-1, SystemDatabase.resolvePollingConcurrency(pooled, -1));

      // Never zero by accident: a one-connection pool still gets a permit.
      pooled.setMaximumPoolSize(1);
      assertEquals(1, SystemDatabase.resolvePollingConcurrency(pooled, null));
    }
  }

  @Test
  @DisplayName("A pool of unknown size falls back to the size DBOS would have created")
  public void resolvesTheDefaultForAForeignDataSource() {
    var opaque = mock(DataSource.class);
    assertEquals(
        SystemDatabase.DEFAULT_POOL_SIZE / 2,
        SystemDatabase.resolvePollingConcurrency(opaque, null));
  }
}
