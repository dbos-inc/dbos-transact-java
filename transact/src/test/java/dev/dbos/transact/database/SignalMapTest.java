package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.database.SignalKey.WakeReason;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class SignalMapTest {

  SignalMap<SignalKey> map;

  static final SignalKey KEY = new SignalKey.Cancellation("wf-1");
  static final SignalKey KEY_A = new SignalKey.Cancellation("wf-a");
  static final SignalKey KEY_B = new SignalKey.Cancellation("wf-b");
  static final SignalKey FOO = new SignalKey.Cancellation("foo");

  @BeforeEach
  void setup() {
    map = new SignalMap<>();
  }

  private static Subscription never() {
    return new Subscription(() -> {});
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
    var f = map.subscribe(KEY, KEY.wakeReason());
    assertFalse(f.isDone());
    map.signal(KEY);
    assertTrue(f.isDone());
    assertFalse(f.isCompletedExceptionally());
    assertEquals(WakeReason.CANCELLED, f.get());
  }

  @Test
  void testMultipleListenersOnSameKey() throws Exception {
    var f1 = map.subscribe(KEY, KEY.wakeReason());
    var f2 = map.subscribe(KEY, KEY.wakeReason());
    var f3 = map.subscribe(KEY, KEY.wakeReason());

    assertFalse(f1.isDone());
    assertFalse(f2.isDone());
    assertFalse(f3.isDone());

    map.signal(KEY);

    assertEquals(WakeReason.CANCELLED, f1.get());
    assertEquals(WakeReason.CANCELLED, f2.get());
    assertEquals(WakeReason.CANCELLED, f3.get());
  }

  @Test
  void testMultipleSubscriptionsInAnyOf() throws Exception {
    var f1 = map.subscribe(KEY_A, KEY_A.wakeReason());
    var f2 = map.subscribe(KEY_B, KEY_B.wakeReason());

    var anyOf = CompletableFuture.anyOf(f1, f2);
    assertFalse(anyOf.isDone());

    map.signal(KEY_B);

    assertEquals(WakeReason.CANCELLED, (WakeReason) anyOf.get(1, TimeUnit.SECONDS));
    assertTrue(f2.isDone());
    assertFalse(f1.isDone());
  }

  @Test
  void testSignalOnlyWakesMatchingKey() {
    var f1 = map.subscribe(KEY_A, KEY_A.wakeReason());
    var f2 = map.subscribe(KEY_B, KEY_B.wakeReason());

    map.signal(KEY_A);

    assertTrue(f1.isDone());
    assertFalse(f2.isDone());
  }

  @Test
  void testDifferentKeyTypesWithSameFieldsDoNotCollide() {
    var eventKey = new SignalKey.Event("wf-1", "wf-1");
    var cancellationKey = new SignalKey.Cancellation("wf-1");

    var f1 = map.subscribe(eventKey, eventKey.wakeReason());
    var f2 = map.subscribe(cancellationKey, cancellationKey.wakeReason());

    map.signal(cancellationKey);

    assertTrue(f2.isDone());
    assertFalse(f1.isDone());
  }

  @Test
  void testSignalBeforeSubscribeDoesNotWake() {
    map.signal(KEY);

    var f = map.subscribe(KEY, KEY.wakeReason());
    assertFalse(f.isDone());

    map.signal(KEY);
    assertTrue(f.isDone());
  }

  @Test
  void testSignalIsOneShot() {
    var f1 = map.subscribe(KEY, KEY.wakeReason());
    map.signal(KEY);
    assertTrue(f1.isDone());

    var f2 = map.subscribe(KEY, KEY.wakeReason());
    assertFalse(f2.isDone());
  }

  @Test
  void testWakeReasonByKeyType() throws Exception {
    var msgSub = map.subscribe(new SignalKey.Message("wf-1", "topic"), WakeReason.MESSAGE);
    var eventSub = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var cancelSub = map.subscribe(new SignalKey.Cancellation("wf-1"), WakeReason.CANCELLED);
    var shutdownSub = map.subscribe(new SignalKey.Shutdown(), WakeReason.SHUTDOWN);

    map.signal(new SignalKey.Message("wf-1", "topic"));
    map.signal(new SignalKey.Event("wf-1", "topic"));
    map.signal(new SignalKey.Cancellation("wf-1"));
    map.signal(new SignalKey.Shutdown());

    assertEquals(WakeReason.MESSAGE, msgSub.get());
    assertEquals(WakeReason.EVENT, eventSub.get());
    assertEquals(WakeReason.CANCELLED, cancelSub.get());
    assertEquals(WakeReason.SHUTDOWN, shutdownSub.get());
  }

  // --- Subscription / close ---

  @Test
  void testCloseOneSubscriberDoesNotOrphanOthers() throws Exception {
    var sub1 = map.subscribe(KEY, KEY.wakeReason());
    var sub2 = map.subscribe(KEY, KEY.wakeReason());

    sub1.close();

    map.signal(KEY);
    assertEquals(WakeReason.CANCELLED, sub2.get());
  }

  @Test
  void testClosePreventsFutureFromBeingSignalled() throws Exception {
    var sub = map.subscribe(KEY, KEY.wakeReason());
    sub.close();
    map.signal(KEY);

    boolean completed =
        sub.orTimeout(100, TimeUnit.MILLISECONDS).handle((v, ex) -> ex == null).get();
    assertFalse(completed);
  }

  @Test
  void testNeverFutureNeverCompletes() throws Exception {
    var f = never();
    assertFalse(f.isDone());

    boolean completed = f.orTimeout(100, TimeUnit.MILLISECONDS).handle((v, ex) -> ex == null).get();
    assertFalse(completed);
  }

  // --- Threading ---

  @Test
  void testSubscribeBeforeSignalFromAnotherThread() throws Exception {
    var f = map.subscribe(FOO, FOO.wakeReason());

    CompletableFuture.runAsync(
        () -> {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          map.signal(FOO);
        });

    assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) f::get);
  }

  @Test
  void testSignalFromMainAfterBackgroundSubscribes() throws Exception {
    var backgroundDone = new CompletableFuture<WakeReason>();

    CompletableFuture.runAsync(
        () -> {
          var f = map.subscribe(FOO, FOO.wakeReason());
          try {
            backgroundDone.complete(f.get(500, TimeUnit.MILLISECONDS));
          } catch (Exception e) {
            backgroundDone.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    map.signal(FOO);

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
            done1.complete(map.subscribe(FOO, FOO.wakeReason()).get(500, TimeUnit.MILLISECONDS));
          } catch (Exception e) {
            done1.completeExceptionally(e);
          }
        });
    CompletableFuture.runAsync(
        () -> {
          try {
            done2.complete(map.subscribe(FOO, FOO.wakeReason()).get(500, TimeUnit.MILLISECONDS));
          } catch (Exception e) {
            done2.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    map.signal(FOO);

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
            var m = new SignalMap<SignalKey>();
            var sub = m.subscribe(KEY, KEY.wakeReason());
            CompletableFuture.runAsync(() -> m.signal(KEY));
            sub.get(1, TimeUnit.SECONDS);
          }
        });
  }

  // --- awaitAny ---

  @Test
  void testAwaitAny_notifyFires() throws Exception {
    var onNotify = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var onCancelled = map.subscribe(new SignalKey.Cancellation("wf-1"), WakeReason.CANCELLED);
    var onShutdown = never();

    map.signal(new SignalKey.Event("wf-1", "topic"));

    assertEquals(
        WakeReason.EVENT,
        SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown));
  }

  @Test
  void testAwaitAny_cancelledFires() throws Exception {
    var onNotify = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var onCancelled = map.subscribe(new SignalKey.Cancellation("wf-1"), WakeReason.CANCELLED);
    var onShutdown = never();

    map.signal(new SignalKey.Cancellation("wf-1"));

    assertEquals(
        WakeReason.CANCELLED,
        SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown));
  }

  @Test
  void testAwaitAny_shutdownFires() throws Exception {
    var onNotify = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var onCancelled = never();
    var onShutdown = map.subscribe(new SignalKey.Shutdown(), WakeReason.SHUTDOWN);

    map.signal(new SignalKey.Shutdown());

    assertEquals(
        WakeReason.SHUTDOWN,
        SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown));
  }

  @Test
  void testAwaitAny_timeout() throws Exception {
    var onNotify = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var onCancelled = never();
    var onShutdown = never();

    assertEquals(
        WakeReason.TIMEOUT,
        SignalMap.awaitAny(Duration.ofMillis(50), onNotify, onCancelled, onShutdown));
  }

  // --- anyOf determination via isDone (Option A) ---

  @Test
  void testCheckIsDone_notifyFires() throws Exception {
    var notifyKey = new SignalKey.Event("wf-1", "topic");
    var cancelKey = new SignalKey.Cancellation("wf-1");

    var onNotify = map.subscribe(notifyKey, notifyKey.wakeReason());
    var onCancelled = map.subscribe(cancelKey, cancelKey.wakeReason());
    var onShutdown = never();

    map.signal(notifyKey);

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

    var onNotify = map.subscribe(notifyKey, notifyKey.wakeReason());
    var onCancelled = map.subscribe(cancelKey, cancelKey.wakeReason());
    var onShutdown = never();

    map.signal(cancelKey);

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertTrue(onCancelled.isDone() || onShutdown.isDone());
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_shutdownFires() throws Exception {
    var onNotify = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var onCancelled = never();
    var onShutdown = map.subscribe(new SignalKey.Shutdown(), WakeReason.SHUTDOWN);

    map.signal(new SignalKey.Shutdown());

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertTrue(onCancelled.isDone() || onShutdown.isDone());
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_timeout() throws Exception {
    var onNotify = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var onCancelled = never();
    var onShutdown = never();

    try {
      CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(50, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException ignored) {
    }

    assertFalse(onCancelled.isDone() || onShutdown.isDone());
    assertFalse(onNotify.isDone());
  }

  // --- anyOf determination via tagged dispatch (Option B) ---

  @Test
  void testTaggedDispatch_notifyFires() throws Exception {
    var notifyKey = new SignalKey.Event("wf-1", "topic");
    var cancelKey = new SignalKey.Cancellation("wf-1");

    var onNotify = map.subscribe(notifyKey, notifyKey.wakeReason());
    var onCancelled = map.subscribe(cancelKey, cancelKey.wakeReason());
    var onShutdown = never();

    map.signal(notifyKey);

    var reason =
        (WakeReason)
            CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.EVENT, reason);
  }

  @Test
  void testTaggedDispatch_cancelledFires() throws Exception {
    var notifyKey = new SignalKey.Event("wf-1", "topic");
    var cancelKey = new SignalKey.Cancellation("wf-1");

    var onNotify = map.subscribe(notifyKey, notifyKey.wakeReason());
    var onCancelled = map.subscribe(cancelKey, cancelKey.wakeReason());
    var onShutdown = never();

    map.signal(cancelKey);

    var reason =
        (WakeReason)
            CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.CANCELLED, reason);
  }

  @Test
  void testTaggedDispatch_shutdownFires() throws Exception {
    var onNotify = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var onCancelled = never();
    var onShutdown = map.subscribe(new SignalKey.Shutdown(), WakeReason.SHUTDOWN);

    map.signal(new SignalKey.Shutdown());

    var reason =
        (WakeReason)
            CompletableFuture.anyOf(onNotify, onCancelled, onShutdown).get(1, TimeUnit.SECONDS);

    assertEquals(WakeReason.SHUTDOWN, reason);
  }

  @Test
  void testTaggedDispatch_timeout() throws Exception {
    var onNotify = map.subscribe(new SignalKey.Event("wf-1", "topic"), WakeReason.EVENT);
    var onCancelled = never();
    var onShutdown = never();

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
