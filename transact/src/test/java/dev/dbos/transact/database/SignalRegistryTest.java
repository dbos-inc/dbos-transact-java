package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

  // Reusable keys
  static final SignalKey KEY = new SignalKey.Cancellation("wf-1");
  static final SignalKey KEY_A = new SignalKey.Cancellation("wf-a");
  static final SignalKey KEY_B = new SignalKey.Cancellation("wf-b");
  static final SignalKey FOO = new SignalKey.Cancellation("foo");

  @BeforeEach
  void setup() {
    registry = new SignalRegistry();
  }

  // --- SignalKey structural equality ---

  @Test
  void testSignalKeyStructuralEquality() {
    assertEquals(new SignalKey.Cancellation("wf-1"), new SignalKey.Cancellation("wf-1"));
    assertEquals(new SignalKey.Event("wf-1", "t"), new SignalKey.Event("wf-1", "t"));
    assertNotEquals(new SignalKey.Cancellation("wf-1"), new SignalKey.Cancellation("wf-2"));
    // Same fields, different types — must not be equal (prevents cross-type collisions)
    assertNotEquals(
        (SignalKey) new SignalKey.Cancellation("wf-1"),
        (SignalKey) new SignalKey.Event("wf-1", "wf-1"));
  }

  // --- Core subscribe / signal behaviour ---

  @Test
  void testBasicSubscribeAndSignal() {
    CompletableFuture<Void> f = registry.subscribe(KEY);
    assertFalse(f.isDone());
    registry.signal(KEY);
    assertTrue(f.isDone());
    assertFalse(f.isCompletedExceptionally());
  }

  @Test
  void testMultipleListenersOnSameKey() {
    CompletableFuture<Void> f1 = registry.subscribe(KEY);
    CompletableFuture<Void> f2 = registry.subscribe(KEY);
    CompletableFuture<Void> f3 = registry.subscribe(KEY);

    assertFalse(f1.isDone());
    assertFalse(f2.isDone());
    assertFalse(f3.isDone());

    registry.signal(KEY);

    assertTrue(f1.isDone());
    assertTrue(f2.isDone());
    assertTrue(f3.isDone());
    assertFalse(f1.isCompletedExceptionally());
    assertFalse(f2.isCompletedExceptionally());
    assertFalse(f3.isCompletedExceptionally());
  }

  @Test
  void testMultipleSubscriptionsInAnyOf() {
    CompletableFuture<Void> f1 = registry.subscribe(KEY_A);
    CompletableFuture<Void> f2 = registry.subscribe(KEY_B);

    CompletableFuture<Object> anyOf = CompletableFuture.anyOf(f1, f2);
    assertFalse(anyOf.isDone());

    registry.signal(KEY_B);

    assertTrue(anyOf.isDone());
    assertTrue(f2.isDone());
    assertFalse(f1.isDone());
  }

  @Test
  void testSignalOnlyWakesMatchingKey() {
    CompletableFuture<Void> f1 = registry.subscribe(KEY_A);
    CompletableFuture<Void> f2 = registry.subscribe(KEY_B);

    registry.signal(KEY_A);

    assertTrue(f1.isDone());
    assertFalse(f2.isDone());
  }

  @Test
  void testDifferentKeyTypesWithSameFieldsDoNotCollide() {
    // Event("wf-1", "wf-1") and Cancellation("wf-1") must occupy separate map entries
    SignalKey eventKey = new SignalKey.Event("wf-1", "wf-1");
    SignalKey cancellationKey = new SignalKey.Cancellation("wf-1");

    CompletableFuture<Void> f1 = registry.subscribe(eventKey);
    CompletableFuture<Void> f2 = registry.subscribe(cancellationKey);

    registry.signal(cancellationKey);

    assertTrue(f2.isDone());
    assertFalse(f1.isDone());
  }

  @Test
  void testSignalBeforeSubscribeDoesNotWake() {
    registry.signal(KEY);

    CompletableFuture<Void> f = registry.subscribe(KEY);
    assertFalse(f.isDone());

    registry.signal(KEY);
    assertTrue(f.isDone());
  }

  @Test
  void testSignalIsOneShot() {
    CompletableFuture<Void> f1 = registry.subscribe(KEY);
    registry.signal(KEY);
    assertTrue(f1.isDone());

    CompletableFuture<Void> f2 = registry.subscribe(KEY);
    assertFalse(f2.isDone());
  }

  // --- Subscription / close ---

  @Test
  void testCloseOneSubscriberDoesNotOrphanOthers() throws Exception {
    // Closing one subscription on a shared key must not prevent the remaining subscriber
    // from being woken when the signal fires (ref-counting behaviour).
    SignalRegistry.Subscription sub1 = registry.subscribe(KEY);
    SignalRegistry.Subscription sub2 = registry.subscribe(KEY);

    sub1.close(); // ref count drops to 1 — key must stay in map

    registry.signal(KEY);
    assertTrue(sub2.isDone());
    assertFalse(sub2.isCompletedExceptionally());
  }

  @Test
  void testClosePreventsFutureFromBeingSignalled() throws Exception {
    SignalRegistry.Subscription sub = registry.subscribe(KEY);
    sub.close();
    registry.signal(KEY); // no entry in map — should be a no-op

    boolean completed =
        sub.orTimeout(100, TimeUnit.MILLISECONDS).handle((v, ex) -> ex == null).get();
    assertFalse(completed);
  }

  @Test
  void testNeverFutureNeverCompletes() throws Exception {
    SignalRegistry.Subscription f = SignalRegistry.never();
    assertFalse(f.isDone());

    boolean completed = f.orTimeout(100, TimeUnit.MILLISECONDS).handle((v, ex) -> ex == null).get();
    assertFalse(completed);
  }

  // --- Threading ---

  @Test
  void testSubscribeBeforeSignalFromAnotherThread() throws Exception {
    CompletableFuture<Void> f = registry.subscribe(FOO);

    CompletableFuture.runAsync(
        () -> {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          registry.signal(FOO);
        });

    assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) f::get);
  }

  @Test
  void testSignalFromMainAfterBackgroundSubscribes() throws Exception {
    CompletableFuture<Void> backgroundDone = new CompletableFuture<>();

    CompletableFuture.runAsync(
        () -> {
          CompletableFuture<Void> f = registry.subscribe(FOO);
          try {
            f.get(500, TimeUnit.MILLISECONDS);
            backgroundDone.complete(null);
          } catch (Exception e) {
            backgroundDone.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    registry.signal(FOO);

    assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) backgroundDone::get);
  }

  @Test
  void testMultipleSubscribersInSeparateThreads() throws Exception {
    CompletableFuture<Void> done1 = new CompletableFuture<>();
    CompletableFuture<Void> done2 = new CompletableFuture<>();

    CompletableFuture.runAsync(
        () -> {
          try {
            registry.subscribe(FOO).get(500, TimeUnit.MILLISECONDS);
            done1.complete(null);
          } catch (Exception e) {
            done1.completeExceptionally(e);
          }
        });
    CompletableFuture.runAsync(
        () -> {
          try {
            registry.subscribe(FOO).get(500, TimeUnit.MILLISECONDS);
            done2.complete(null);
          } catch (Exception e) {
            done2.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    registry.signal(FOO);

    assertTimeoutPreemptively(
        Duration.ofSeconds(1),
        () -> {
          done1.get();
          done2.get();
        });
  }

  @Test
  void testConcurrentSignalAndSubscribe() throws Exception {
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          for (int i = 0; i < 1000; i++) {
            SignalRegistry r = new SignalRegistry();
            CompletableFuture<Void> sub = r.subscribe(KEY);
            CompletableFuture.runAsync(() -> r.signal(KEY));
            sub.get(1, TimeUnit.SECONDS);
          }
        });
  }

  // --- keys() ---

  @Test
  void testKeysReflectsActiveSubscriptions() {
    registry.subscribe(KEY_A);
    registry.subscribe(KEY_B);

    Iterable<SignalKey> keys = registry.keys();
    assertTrue(iterableContains(keys, KEY_A));
    assertTrue(iterableContains(keys, KEY_B));
  }

  @Test
  void testKeysExcludesSignalledKey() {
    registry.subscribe(KEY_A);
    registry.subscribe(KEY_B);
    registry.signal(KEY_A);

    Iterable<SignalKey> keys = registry.keys();
    assertFalse(iterableContains(keys, KEY_A));
    assertTrue(iterableContains(keys, KEY_B));
  }

  @Test
  void testKeysEmptyWhenNoSubscribers() {
    assertFalse(registry.keys().iterator().hasNext());
  }

  @Test
  void testKeysExcludesKeyAfterLastSubscriberCloses() {
    SignalRegistry.Subscription sub = registry.subscribe(KEY);
    sub.close();
    assertFalse(registry.keys().iterator().hasNext());
  }

  private static boolean iterableContains(Iterable<SignalKey> keys, SignalKey target) {
    for (SignalKey k : keys) {
      if (k.equals(target)) return true;
    }
    return false;
  }

  // --- anyOf determination via isDone (Option A) ---

  @Test
  void testCheckIsDone_notifyFires() throws Exception {
    SignalKey notifyKey = new SignalKey.Event("wf-1", "topic");
    SignalKey cancelKey = new SignalKey.Cancellation("wf-1");

    CompletableFuture<Void> onNotify = registry.subscribe(notifyKey);
    CompletableFuture<Void> onCancelled = registry.subscribe(cancelKey);
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    registry.signal(notifyKey);

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onDbClosed).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onCancelled.isDone() || onDbClosed.isDone());
    assertTrue(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_cancelledFires() throws Exception {
    SignalKey notifyKey = new SignalKey.Event("wf-1", "topic");
    SignalKey cancelKey = new SignalKey.Cancellation("wf-1");

    CompletableFuture<Void> onNotify = registry.subscribe(notifyKey);
    CompletableFuture<Void> onCancelled = registry.subscribe(cancelKey);
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    registry.signal(cancelKey);

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onDbClosed).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertTrue(onCancelled.isDone() || onDbClosed.isDone());
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_dbClosedFires() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    CompletableFuture<Void> onCancelled = SignalRegistry.never();
    CompletableFuture<Void> onDbClosed = new CompletableFuture<>();
    onDbClosed.complete(null);

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onDbClosed).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertTrue(onCancelled.isDone() || onDbClosed.isDone());
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_timeout() throws Exception {
    CompletableFuture<Void> onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    CompletableFuture<Void> onCancelled = SignalRegistry.never();
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onDbClosed).get(50, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onCancelled.isDone() || onDbClosed.isDone());
    assertFalse(onNotify.isDone());
  }

  // --- anyOf determination via tagged dispatch (Option B) ---

  @Test
  void testTaggedDispatch_notifyFires() throws Exception {
    SignalKey notifyKey = new SignalKey.Event("wf-1", "topic");
    SignalKey cancelKey = new SignalKey.Cancellation("wf-1");

    CompletableFuture<Void> onNotify = registry.subscribe(notifyKey);
    CompletableFuture<Void> onCancelled = registry.subscribe(cancelKey);
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    registry.signal(notifyKey);

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
    SignalKey notifyKey = new SignalKey.Event("wf-1", "topic");
    SignalKey cancelKey = new SignalKey.Cancellation("wf-1");

    CompletableFuture<Void> onNotify = registry.subscribe(notifyKey);
    CompletableFuture<Void> onCancelled = registry.subscribe(cancelKey);
    CompletableFuture<Void> onDbClosed = SignalRegistry.never();

    registry.signal(cancelKey);

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
    CompletableFuture<Void> onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
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
    CompletableFuture<Void> onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
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
    assertEquals(null, reason);
  }
}
