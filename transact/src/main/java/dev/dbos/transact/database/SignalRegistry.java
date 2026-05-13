package dev.dbos.transact.database;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

sealed interface SignalKey
    permits SignalKey.Cancellation, SignalKey.Event, SignalKey.Message, SignalKey.Shutdown {

  record Cancellation(String workflowId) implements SignalKey {}

  record Event(String workflowId, String topic) implements SignalKey {}

  record Message(String workflowId, String topic) implements SignalKey {}

  record Shutdown() implements SignalKey {}
}

class SignalRegistry {

  private static class Entry {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    final AtomicInteger refs = new AtomicInteger(1);
  }

  static class Subscription extends CompletableFuture<Void> implements AutoCloseable {
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
    entry.future.thenRun(() -> sub.complete(null));
    return sub;
  }

  public void signal(SignalKey key) {
    Entry e = map.remove(key);
    if (e != null) e.future.complete(null);
  }

  Iterable<SignalKey> keys() {
    return map.keySet();
  }

  static Subscription never() {
    return new Subscription(() -> {});
  }
}
