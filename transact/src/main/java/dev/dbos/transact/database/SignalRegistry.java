package dev.dbos.transact.database;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

  private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();

  Subscription subscribe(String key) {
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

  void signal(String key) {
    Entry e = map.remove(key);
    if (e != null) e.future.complete(null);
  }

  static CompletableFuture<Void> never() {
    return new CompletableFuture<>();
  }
}
