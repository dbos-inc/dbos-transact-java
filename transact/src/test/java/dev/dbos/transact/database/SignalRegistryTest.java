package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    assertNotEquals(
        (SignalKey) new SignalKey.Cancellation("wf-1"),
        (SignalKey) new SignalKey.Event("wf-1", "wf-1"));
  }

  // --- Core subscribe / signal behaviour ---

  @Test
  void testBasicSubscribeAndSignal() throws Exception {
    var f = registry.subscribe(KEY);
    assertFalse(f.isDone());
    registry.signal(KEY);
    assertTrue(f.isDone());
    assertFalse(f.isCompletedExceptionally());
    assertEquals(WakeReason.CANCELLED, f.get());
  }

  @Test
  void testMultipleListenersOnSameKey() throws Exception {
    var f1 = registry.subscribe(KEY);
    var f2 = registry.subscribe(KEY);
    var f3 = registry.subscribe(KEY);

    assertFalse(f1.isDone());
    assertFalse(f2.isDone());
    assertFalse(f3.isDone());

    registry.signal(KEY);

    assertEquals(WakeReason.CANCELLED, f1.get());
    assertEquals(WakeReason.CANCELLED, f2.get());
    assertEquals(WakeReason.CANCELLED, f3.get());
  }

  @Test
  void testMultipleSubscriptionsInAnyOf() throws Exception {
    var f1 = registry.subscribe(KEY_A);
    var f2 = registry.subscribe(KEY_B);

    var anyOf = CompletableFuture.anyOf(f1, f2);
    assertFalse(anyOf.isDone());

    registry.signal(KEY_B);

    assertEquals(WakeReason.CANCELLED, (WakeReason) anyOf.get(1, TimeUnit.SECONDS));
    assertTrue(f2.isDone());
    assertFalse(f1.isDone());
  }

  @Test
  void testSignalOnlyWakesMatchingKey() {
    var f1 = registry.subscribe(KEY_A);
    var f2 = registry.subscribe(KEY_B);

    registry.signal(KEY_A);

    assertTrue(f1.isDone());
    assertFalse(f2.isDone());
  }

  @Test
  void testDifferentKeyTypesWithSameFieldsDoNotCollide() {
    var eventKey = new SignalKey.Event("wf-1", "wf-1");
    var cancellationKey = new SignalKey.Cancellation("wf-1");

    var f1 = registry.subscribe(eventKey);
    var f2 = registry.subscribe(cancellationKey);

    registry.signal(cancellationKey);

    assertTrue(f2.isDone());
    assertFalse(f1.isDone());
  }

  @Test
  void testSignalBeforeSubscribeDoesNotWake() {
    registry.signal(KEY);

    var f = registry.subscribe(KEY);
    assertFalse(f.isDone());

    registry.signal(KEY);
    assertTrue(f.isDone());
  }

  @Test
  void testSignalIsOneShot() {
    var f1 = registry.subscribe(KEY);
    registry.signal(KEY);
    assertTrue(f1.isDone());

    var f2 = registry.subscribe(KEY);
    assertFalse(f2.isDone());
  }

  @Test
  void testWakeReasonByKeyType() throws Exception {
    var msgSub = registry.subscribe(new SignalKey.Message("wf-1", "topic"));
    var eventSub = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var cancelSub = registry.subscribe(new SignalKey.Cancellation("wf-1"));
    var shutdownSub = registry.subscribe(new SignalKey.Shutdown());

    registry.signal(new SignalKey.Message("wf-1", "topic"));
    registry.signal(new SignalKey.Event("wf-1", "topic"));
    registry.signal(new SignalKey.Cancellation("wf-1"));
    registry.signal(new SignalKey.Shutdown());

    assertEquals(WakeReason.MESSAGE, msgSub.get());
    assertEquals(WakeReason.EVENT, eventSub.get());
    assertEquals(WakeReason.CANCELLED, cancelSub.get());
    assertEquals(WakeReason.SHUTDOWN, shutdownSub.get());
  }

  // --- Subscription / close ---

  @Test
  void testCloseOneSubscriberDoesNotOrphanOthers() throws Exception {
    var sub1 = registry.subscribe(KEY);
    var sub2 = registry.subscribe(KEY);

    sub1.close();

    registry.signal(KEY);
    assertEquals(WakeReason.CANCELLED, sub2.get());
  }

  @Test
  void testClosePreventsFutureFromBeingSignalled() throws Exception {
    var sub = registry.subscribe(KEY);
    sub.close();
    registry.signal(KEY);

    boolean completed =
        sub.orTimeout(100, TimeUnit.MILLISECONDS).handle((v, ex) -> ex == null).get();
    assertFalse(completed);
  }

  @Test
  void testNeverFutureNeverCompletes() throws Exception {
    var f = SignalRegistry.never();
    assertFalse(f.isDone());

    boolean completed = f.orTimeout(100, TimeUnit.MILLISECONDS).handle((v, ex) -> ex == null).get();
    assertFalse(completed);
  }

  // --- keys() ---

  @Test
  void testKeysReflectsActiveSubscriptions() {
    registry.subscribe(KEY_A);
    registry.subscribe(KEY_B);

    var keys = registry.keys();
    assertTrue(iterableContains(keys, KEY_A));
    assertTrue(iterableContains(keys, KEY_B));
  }

  @Test
  void testKeysExcludesSignalledKey() {
    registry.subscribe(KEY_A);
    registry.subscribe(KEY_B);
    registry.signal(KEY_A);

    var keys = registry.keys();
    assertFalse(iterableContains(keys, KEY_A));
    assertTrue(iterableContains(keys, KEY_B));
  }

  @Test
  void testKeysEmptyWhenNoSubscribers() {
    assertFalse(registry.keys().iterator().hasNext());
  }

  @Test
  void testKeysExcludesKeyAfterLastSubscriberCloses() {
    var sub = registry.subscribe(KEY);
    sub.close();
    assertFalse(registry.keys().iterator().hasNext());
  }

  private static boolean iterableContains(Iterable<SignalKey> keys, SignalKey target) {
    for (SignalKey k : keys) {
      if (k.equals(target)) return true;
    }
    return false;
  }

  // --- Threading ---

  @Test
  void testSubscribeBeforeSignalFromAnotherThread() throws Exception {
    var f = registry.subscribe(FOO);

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
    var backgroundDone = new CompletableFuture<WakeReason>();

    CompletableFuture.runAsync(
        () -> {
          var f = registry.subscribe(FOO);
          try {
            backgroundDone.complete(f.get(500, TimeUnit.MILLISECONDS));
          } catch (Exception e) {
            backgroundDone.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    registry.signal(FOO);

    assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) backgroundDone::get);
    assertEquals(WakeReason.CANCELLED, backgroundDone.get());
  }

  @Test
  void testMultipleSubscribersInSeparateThreads() throws Exception {
    var done1 = new CompletableFuture<WakeReason>();
    var done2 = new CompletableFuture<WakeReason>();

    CompletableFuture.runAsync(
        () -> {
          try {
            done1.complete(registry.subscribe(FOO).get(500, TimeUnit.MILLISECONDS));
          } catch (Exception e) {
            done1.completeExceptionally(e);
          }
        });
    CompletableFuture.runAsync(
        () -> {
          try {
            done2.complete(registry.subscribe(FOO).get(500, TimeUnit.MILLISECONDS));
          } catch (Exception e) {
            done2.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    registry.signal(FOO);

    assertTimeoutPreemptively(
        Duration.ofSeconds(1),
        () -> {
          assertEquals(WakeReason.CANCELLED, done1.get());
          assertEquals(WakeReason.CANCELLED, done2.get());
        });
  }

  @Test
  void testConcurrentSignalAndSubscribe() throws Exception {
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          for (int i = 0; i < 1000; i++) {
            var r = new SignalRegistry();
            var sub = r.subscribe(KEY);
            CompletableFuture.runAsync(() -> r.signal(KEY));
            sub.get(1, TimeUnit.SECONDS);
          }
        });
  }

  // --- awaitAny ---

  @Test
  void testAwaitAny_notifyFires() throws Exception {
    var onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var onCancelled = registry.subscribe(new SignalKey.Cancellation("wf-1"));
    var onShutdown = SignalRegistry.never();

    registry.signal(new SignalKey.Event("wf-1", "topic"));

    assertEquals(
        WakeReason.EVENT,
        SignalRegistry.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown));
  }

  @Test
  void testAwaitAny_cancelledFires() throws Exception {
    var onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var onCancelled = registry.subscribe(new SignalKey.Cancellation("wf-1"));
    var onShutdown = SignalRegistry.never();

    registry.signal(new SignalKey.Cancellation("wf-1"));

    assertEquals(
        WakeReason.CANCELLED,
        SignalRegistry.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown));
  }

  @Test
  void testAwaitAny_shutdownFires() throws Exception {
    var onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var onCancelled = SignalRegistry.never();
    var onShutdown = registry.subscribe(new SignalKey.Shutdown());

    registry.signal(new SignalKey.Shutdown());

    assertEquals(
        WakeReason.SHUTDOWN,
        SignalRegistry.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown));
  }

  @Test
  void testAwaitAny_timeout() throws Exception {
    var onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var onCancelled = SignalRegistry.never();
    var onShutdown = SignalRegistry.never();

    assertEquals(
        WakeReason.TIMEOUT,
        SignalRegistry.awaitAny(Duration.ofMillis(50), onNotify, onCancelled, onShutdown));
  }

  // --- anyOf determination via isDone (Option A) ---

  @Test
  void testCheckIsDone_notifyFires() throws Exception {
    var notifyKey = new SignalKey.Event("wf-1", "topic");
    var cancelKey = new SignalKey.Cancellation("wf-1");

    var onNotify = registry.subscribe(notifyKey);
    var onCancelled = registry.subscribe(cancelKey);
    var onShutdown = SignalRegistry.never();

    registry.signal(notifyKey);

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onCancelled.isDone() || onShutdown.isDone());
    assertTrue(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_cancelledFires() throws Exception {
    var notifyKey = new SignalKey.Event("wf-1", "topic");
    var cancelKey = new SignalKey.Cancellation("wf-1");

    var onNotify = registry.subscribe(notifyKey);
    var onCancelled = registry.subscribe(cancelKey);
    var onShutdown = SignalRegistry.never();

    registry.signal(cancelKey);

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertTrue(onCancelled.isDone() || onShutdown.isDone());
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_shutdownFires() throws Exception {
    var onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var onCancelled = SignalRegistry.never();
    var onShutdown = registry.subscribe(new SignalKey.Shutdown());

    registry.signal(new SignalKey.Shutdown());

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertTrue(onCancelled.isDone() || onShutdown.isDone());
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_timeout() throws Exception {
    var onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var onCancelled = SignalRegistry.never();
    var onShutdown = SignalRegistry.never();

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(50, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onCancelled.isDone() || onShutdown.isDone());
    assertFalse(onNotify.isDone());
  }

  // --- anyOf determination via tagged dispatch (Option B) ---
  // WakeReason is now embedded in each Subscription — no thenApply needed.

  @Test
  void testTaggedDispatch_notifyFires() throws Exception {
    var notifyKey = new SignalKey.Event("wf-1", "topic");
    var cancelKey = new SignalKey.Cancellation("wf-1");

    var onNotify = registry.subscribe(notifyKey);
    var onCancelled = registry.subscribe(cancelKey);
    var onShutdown = SignalRegistry.never();

    registry.signal(notifyKey);

    var reason =
        (WakeReason)
            CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.EVENT, reason);
  }

  @Test
  void testTaggedDispatch_cancelledFires() throws Exception {
    var notifyKey = new SignalKey.Event("wf-1", "topic");
    var cancelKey = new SignalKey.Cancellation("wf-1");

    var onNotify = registry.subscribe(notifyKey);
    var onCancelled = registry.subscribe(cancelKey);
    var onShutdown = SignalRegistry.never();

    registry.signal(cancelKey);

    var reason =
        (WakeReason)
            CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.CANCELLED, reason);
  }

  @Test
  void testTaggedDispatch_shutdownFires() throws Exception {
    var onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var onCancelled = SignalRegistry.never();
    var onShutdown = registry.subscribe(new SignalKey.Shutdown());

    registry.signal(new SignalKey.Shutdown());

    var reason =
        (WakeReason)
            CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.SHUTDOWN, reason);
  }

  @Test
  void testTaggedDispatch_timeout() throws Exception {
    var onNotify = registry.subscribe(new SignalKey.Event("wf-1", "topic"));
    var onCancelled = SignalRegistry.never();
    var onShutdown = SignalRegistry.never();

    WakeReason reason = null;
    try {
      reason =
          (WakeReason)
              CompletableFuture.anyOf(onNotify, onCancelled, onShutdown)
                  .get(50, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onNotify.isDone());
    assertNull(reason);
  }
}
