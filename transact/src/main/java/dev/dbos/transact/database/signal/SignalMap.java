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
