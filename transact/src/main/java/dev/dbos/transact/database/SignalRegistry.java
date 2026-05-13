package dev.dbos.transact.database;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

class SignalRegistry {

  private final ConcurrentHashMap<String, CompletableFuture<Void>> map = new ConcurrentHashMap<>();

  CompletableFuture<Void> subscribe(String key) {
    return map.computeIfAbsent(key, k -> new CompletableFuture<>()).copy();
  }

  void signal(String key) {
    CompletableFuture<Void> f = map.remove(key);
    if (f != null) f.complete(null);
  }

  void unsubscribe(String key) {
    map.remove(key);
  }

  static CompletableFuture<Void> never() {
    return new CompletableFuture<>();
  }
}
