package dev.dbos.transact.database;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

sealed interface SignalKey
    permits SignalKey.Cancellation, SignalKey.Event, SignalKey.Message, SignalKey.Shutdown {

  WakeReason wakeReason();

  record Cancellation(String workflowId) implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.CANCELLED;
    }
  }

  record Event(String workflowId, String topic) implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.EVENT;
    }
  }

  record Message(String workflowId, String topic) implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.MESSAGE;
    }
  }

  record Shutdown() implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.SHUTDOWN;
    }
  }
}

enum WakeReason {
  MESSAGE,
  EVENT,
  CANCELLED,
  SHUTDOWN,
  TIMEOUT
}

class SignalRegistry {

  private static class Entry {
    final CompletableFuture<WakeReason> future = new CompletableFuture<>();
    final AtomicInteger refs = new AtomicInteger(1);
  }

  static class Subscription extends CompletableFuture<WakeReason> implements AutoCloseable {
    private final Runnable onClose;

    Subscription(Runnable onClose) {
      this.onClose = onClose;
    }

    @Override
    public void close() {
      onClose.run();
    }
  }

  private final ConcurrentHashMap<SignalKey, Entry> map = new ConcurrentHashMap<>();

  public Subscription subscribe(SignalKey key) {
    Entry entry =
        map.compute(
            key,
            (k, e) -> {
              if (e != null) {
                e.refs.incrementAndGet();
                return e;
              }
              return new Entry();
            });
    Subscription sub =
        new Subscription(
            () ->
                map.compute(
                    key,
                    (k, e) -> {
                      if (e != null && e.refs.decrementAndGet() == 0) return null;
                      return e;
                    }));
    entry.future.thenAccept(sub::complete);
    return sub;
  }

  public void signal(SignalKey key) {
    Entry e = map.remove(key);
    if (e != null) e.future.complete(key.wakeReason());
  }

  Iterable<SignalKey> keys() {
    return map.keySet();
  }

  static Subscription never() {
    return new Subscription(() -> {});
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
