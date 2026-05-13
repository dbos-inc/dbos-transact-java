package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class SignalRegistryTest {

  SignalRegistry registry;

  @BeforeEach
  void setup() {
    registry = new SignalRegistry();
  }

  @Test
  void testBasicSubscribeAndSignal() {
    CompletableFuture<Void> f = registry.subscribe("key");
    assertFalse(f.isDone());
    registry.signal("key");
    assertTrue(f.isDone());
    assertFalse(f.isCompletedExceptionally());
  }

  @Test
  void testMultipleListenersOnSameKey() {
    // Multiple subscribers on the same key must all complete when signal fires
    CompletableFuture<Void> f1 = registry.subscribe("key");
    CompletableFuture<Void> f2 = registry.subscribe("key");
    CompletableFuture<Void> f3 = registry.subscribe("key");

    assertFalse(f1.isDone());
    assertFalse(f2.isDone());
    assertFalse(f3.isDone());

    registry.signal("key");

    assertTrue(f1.isDone());
    assertTrue(f2.isDone());
    assertTrue(f3.isDone());
    assertFalse(f1.isCompletedExceptionally());
    assertFalse(f2.isCompletedExceptionally());
    assertFalse(f3.isCompletedExceptionally());
  }

  @Test
  void testMultipleSubscriptionsInAnyOf() {
    // anyOf(sub1, sub2) — signalling sub2's key should complete the anyOf block
    CompletableFuture<Void> f1 = registry.subscribe("key-one");
    CompletableFuture<Void> f2 = registry.subscribe("key-two");

    CompletableFuture<Object> anyOf = CompletableFuture.anyOf(f1, f2);
    assertFalse(anyOf.isDone());

    registry.signal("key-two");

    assertTrue(anyOf.isDone());
    assertTrue(f2.isDone());
    assertFalse(f1.isDone()); // key-one was never signalled
  }

  @Test
  void testSignalOnlyWakesMatchingKey() {
    CompletableFuture<Void> f1 = registry.subscribe("key-one");
    CompletableFuture<Void> f2 = registry.subscribe("key-two");

    registry.signal("key-one");

    assertTrue(f1.isDone());
    assertFalse(f2.isDone());
  }

  @Test
  void testSignalBeforeSubscribeDoesNotWake() {
    // signal fires with no subscribers; subsequent subscribe gets a fresh future
    registry.signal("key");

    CompletableFuture<Void> f = registry.subscribe("key");
    assertFalse(f.isDone());

    // verify it can still be signalled normally afterwards
    registry.signal("key");
    assertTrue(f.isDone());
  }

  @Test
  void testSignalIsOneShot() {
    CompletableFuture<Void> f1 = registry.subscribe("key");
    registry.signal("key");
    assertTrue(f1.isDone());

    // second signal on same key — new subscriber should need a new signal
    CompletableFuture<Void> f2 = registry.subscribe("key");
    assertFalse(f2.isDone());
  }

  @Test
  void testCloseOneSubscriberDoesNotOrphanOthers() throws Exception {
    // Closing one subscription on a shared key must not prevent the remaining subscriber
    // from being woken when the signal fires (ref-counting behaviour).
    SignalRegistry.Subscription sub1 = registry.subscribe("key");
    SignalRegistry.Subscription sub2 = registry.subscribe("key");

    sub1.close(); // ref count drops to 1 — key must stay in map

    registry.signal("key");
    assertTrue(sub2.isDone());
    assertFalse(sub2.isCompletedExceptionally());
  }

  @Test
  void testClosePreventsFutureFromBeingSignalled() throws Exception {
    SignalRegistry.Subscription sub = registry.subscribe("key");
    sub.close();
    registry.signal("key"); // no entry in map — should be a no-op

    // sub was closed before signal so the shared future was removed from the map;
    // it will never complete since nothing holds a reference to complete it
    boolean completed =
        sub.orTimeout(100, TimeUnit.MILLISECONDS).handle((v, ex) -> ex == null).get();
    assertFalse(completed);
  }

  @Test
  void testNeverFutureNeverCompletes() throws Exception {
    CompletableFuture<Void> f = SignalRegistry.never();
    assertFalse(f.isDone());

    boolean completed = f.orTimeout(100, TimeUnit.MILLISECONDS).handle((v, ex) -> ex == null).get();
    assertFalse(completed);
  }

  @Test
  void testSubscribeBeforeSignalFromAnotherThread() throws Exception {
    // Subscribe on the current thread, signal from a background thread after a delay.
    CompletableFuture<Void> f = registry.subscribe("foo");

    CompletableFuture.runAsync(
        () -> {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          registry.signal("foo");
        });

    assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) f::get);
  }

  @Test
  void testSignalFromMainAfterBackgroundSubscribes() throws Exception {
    // Background thread subscribes and waits; main thread signals after a delay.
    CompletableFuture<Void> backgroundDone = new CompletableFuture<>();

    CompletableFuture.runAsync(
        () -> {
          CompletableFuture<Void> f = registry.subscribe("foo");
          try {
            f.get(500, TimeUnit.MILLISECONDS);
            backgroundDone.complete(null);
          } catch (Exception e) {
            backgroundDone.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    registry.signal("foo");

    assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) backgroundDone::get);
  }

  @Test
  void testMultipleSubscribersInSeparateThreads() throws Exception {
    // Two background threads each subscribe; a single signal wakes both.
    CompletableFuture<Void> done1 = new CompletableFuture<>();
    CompletableFuture<Void> done2 = new CompletableFuture<>();

    CompletableFuture.runAsync(
        () -> {
          try {
            registry.subscribe("foo").get(500, TimeUnit.MILLISECONDS);
            done1.complete(null);
          } catch (Exception e) {
            done1.completeExceptionally(e);
          }
        });
    CompletableFuture.runAsync(
        () -> {
          try {
            registry.subscribe("foo").get(500, TimeUnit.MILLISECONDS);
            done2.complete(null);
          } catch (Exception e) {
            done2.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    registry.signal("foo");

    assertTimeoutPreemptively(
        Duration.ofSeconds(1),
        () -> {
          done1.get();
          done2.get();
        });
  }

  @Test
  void testConcurrentSignalAndSubscribe() throws Exception {
    // Stress: signal and subscribe racing from different threads should not deadlock or lose
    // wakeups
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          for (int i = 0; i < 1000; i++) {
            SignalRegistry r = new SignalRegistry();
            CompletableFuture<Void> sub = r.subscribe("key");
            CompletableFuture.runAsync(() -> r.signal("key"));
            sub.get(1, TimeUnit.SECONDS);
          }
        });
  }

  // --- anyOf determination via checking isDone

  @Test
  void testCheckIsDone_notifyFires() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe("notify-key");
    CompletableFuture<Void> onCancelled = registry.subscribe("cancel-key");
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    registry.signal("notify-key");

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onDbClosed).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onCancelled.isDone() || onDbClosed.isDone()); // routes to re-check-DB branch
    assertTrue(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_cancelledFires() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe("notify-key");
    CompletableFuture<Void> onCancelled = registry.subscribe("cancel-key");
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    registry.signal("cancel-key");

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onDbClosed).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertTrue(onCancelled.isDone() || onDbClosed.isDone()); // routes to return-null branch
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_dbClosedFires() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe("notify-key");
    CompletableFuture<Void> onCancelled = SignalRegistry.never();
    CompletableFuture<Void> onDbClosed = new CompletableFuture<>();
    onDbClosed.complete(null);

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onDbClosed).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertTrue(onCancelled.isDone() || onDbClosed.isDone()); // routes to return-null branch
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_timeout() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe("notify-key");
    CompletableFuture<Void> onCancelled = SignalRegistry.never();
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onDbClosed).get(50, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onCancelled.isDone() || onDbClosed.isDone()); // routes to re-check-DB branch
    assertFalse(onNotify.isDone());
  }

  // --- anyOf determination via checking isDone via tagged dispatch

  @Test
  void testTaggedDispatch_notifyFires() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe("notify-key");
    CompletableFuture<Void> onCancelled = registry.subscribe("cancel-key");
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    registry.signal("notify-key");

    enum WakeReason {
      NOTIFY,
      CANCELLED,
      DB_CLOSED
    }
    WakeReason reason =
        (WakeReason)
            CompletableFuture.anyOf(
                    onNotify.thenApply(v -> WakeReason.NOTIFY),
                    onCancelled.thenApply(v -> WakeReason.CANCELLED),
                    onDbClosed.thenApply(v -> WakeReason.DB_CLOSED))
                .get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.NOTIFY, reason);
  }

  @Test
  void testTaggedDispatch_cancelledFires() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe("notify-key");
    CompletableFuture<Void> onCancelled = registry.subscribe("cancel-key");
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    registry.signal("cancel-key");

    enum WakeReason {
      NOTIFY,
      CANCELLED,
      DB_CLOSED
    }
    WakeReason reason =
        (WakeReason)
            CompletableFuture.anyOf(
                    onNotify.thenApply(v -> WakeReason.NOTIFY),
                    onCancelled.thenApply(v -> WakeReason.CANCELLED),
                    onDbClosed.thenApply(v -> WakeReason.DB_CLOSED))
                .get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.CANCELLED, reason);
  }

  @Test
  void testTaggedDispatch_dbClosedFires() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe("notify-key");
    CompletableFuture<Void> onCancelled = SignalRegistry.never();
    CompletableFuture<Void> onDbClosed = new CompletableFuture<>();
    onDbClosed.complete(null);

    enum WakeReason {
      NOTIFY,
      CANCELLED,
      DB_CLOSED
    }
    WakeReason reason =
        (WakeReason)
            CompletableFuture.anyOf(
                    onNotify.thenApply(v -> WakeReason.NOTIFY),
                    onCancelled.thenApply(v -> WakeReason.CANCELLED),
                    onDbClosed.thenApply(v -> WakeReason.DB_CLOSED))
                .get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.DB_CLOSED, reason);
  }

  @Test
  void testTaggedDispatch_timeout() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe("notify-key");
    CompletableFuture<Void> onCancelled = SignalRegistry.never();
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    enum WakeReason {
      NOTIFY,
      CANCELLED,
      DB_CLOSED
    }
    WakeReason reason = null;
    try {
      reason =
          (WakeReason)
              CompletableFuture.anyOf(
                      onNotify.thenApply(v -> WakeReason.NOTIFY),
                      onCancelled.thenApply(v -> WakeReason.CANCELLED),
                      onDbClosed.thenApply(v -> WakeReason.DB_CLOSED))
                  .get(50, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onNotify.isDone());
    assertEquals(null, reason); // anyOf threw — no wake reason was tagged
  }
}
