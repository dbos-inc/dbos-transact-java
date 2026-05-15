package dev.dbos.transact.database;

import dev.dbos.transact.database.SignalKey.WakeReason;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

class SignalMap<K> {
  private static class Entry {
    final CompletableFuture<WakeReason> future = new CompletableFuture<>();
    final AtomicInteger refs = new AtomicInteger(1);
    final WakeReason reason;

    public Entry(WakeReason reason) {
      this.reason = Objects.requireNonNull(reason);
    }
  }

  private final ConcurrentHashMap<K, Entry> map = new ConcurrentHashMap<>();

  public Subscription subscribe(K key, WakeReason reason) {
    var entry =
        map.compute(
            key,
            (k, e) -> {
              if (e != null) {
                e.refs.incrementAndGet();
                return e;
              }
              return new Entry(reason);
            });

    var sub =
        new Subscription(
            () ->
                map.compute(key, (k, e) -> e != null && e.refs.decrementAndGet() == 0 ? null : e));

    entry.future.thenAccept(sub::complete);
    return sub;
  }

  public void signal(K key) {
    var e = map.remove(key);
    if (e != null) {
      e.future.complete(e.reason);
    }
  }

  static WakeReason awaitAny(Duration timeout, Subscription... subscriptions) {
    try {
      return (WakeReason)
          CompletableFuture.anyOf(subscriptions).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException ignored) {
      return WakeReason.TIMEOUT;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
