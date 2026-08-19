package dev.dbos.transact.database.signal;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class SignalMap {
  private static class Entry {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    final AtomicInteger refs = new AtomicInteger(1);
  }

  private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();

  public Subscription subscribe(String key) {
    var entry =
        map.compute(
            key,
            (k, e) -> {
              if (e != null) {
                e.refs.incrementAndGet();
                return e;
              }
              return new Entry();
            });

    var sub =
        new Subscription(
            () ->
                map.compute(key, (k, e) -> e != null && e.refs.decrementAndGet() == 0 ? null : e));

    entry.future.thenAccept(
        ignored -> {
          if (!sub.closed) {
            sub.complete(null);
          }
        });
    return sub;
  }

  public void raiseSignal(String key) {
    var e = map.remove(key);
    if (e != null) {
      e.future.complete(null);
    }
  }

  /**
   * Wake every outstanding waiter, so each re-queries once.
   *
   * <p>For use after (re)establishing LISTEN. A NOTIFY that fires while nothing is listening is
   * lost for good -- re-subscribing only catches later ones -- so a waiter whose row was written
   * during the outage would otherwise learn nothing until its next re-check, which for a wait with
   * a short timeout may never come. Waking everyone costs one query each and is bounded by the
   * polling limiter.
   */
  public void raiseAll() {
    // Weakly consistent iteration is fine: raiseSignal removes as it goes, and a key added
    // concurrently belongs to a waiter that subscribed after the reconnect.
    for (var key : map.keySet()) {
      raiseSignal(key);
    }
  }

  public static void awaitAny(Duration timeout, Subscription... subscriptions) {
    try {
      CompletableFuture.anyOf(subscriptions).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException ignored) {
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
