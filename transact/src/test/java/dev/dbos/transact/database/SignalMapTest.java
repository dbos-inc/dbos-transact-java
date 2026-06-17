package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.database.signal.SignalMap;
import dev.dbos.transact.database.signal.Subscription;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class SignalMapTest {

  SignalMap map;

  static final String KEY = "c::wf-1";
  static final String KEY_A = "c::wf-a";
  static final String KEY_B = "c::wf-b";
  static final String FOO = "c::foo";

  @BeforeEach
  void setup() {
    map = new SignalMap();
  }

  private static Subscription never() {
    return new Subscription(() -> {});
  }

  // --- SignalKey structural equality ---

  @Test
  void testSignalKeyStructuralEquality() {
    assertEquals(new SignalKey.Message("wf-1", "t"), new SignalKey.Message("wf-1", "t"));
    assertEquals(new SignalKey.Event("wf-1", "t"), new SignalKey.Event("wf-1", "t"));
    assertNotEquals(new SignalKey.Event("wf-1", "t"), new SignalKey.Event("wf-2", "t"));
    assertNotEquals(
        (SignalKey) new SignalKey.Message("wf-1", "wf-1"),
        (SignalKey) new SignalKey.Event("wf-1", "wf-1"));
  }

  // --- Core subscribe / signal behaviour ---

  @Test
  void testBasicSubscribeAndSignal() throws Exception {
    var f = map.subscribe(KEY);
    assertFalse(f.isDone());
    map.raiseSignal(KEY);
    assertTrue(f.isDone());
    assertFalse(f.isCompletedExceptionally());
  }

  @Test
  void testMultipleListenersOnSameKey() throws Exception {
    var f1 = map.subscribe(KEY);
    var f2 = map.subscribe(KEY);
    var f3 = map.subscribe(KEY);

    assertFalse(f1.isDone());
    assertFalse(f2.isDone());
    assertFalse(f3.isDone());

    map.raiseSignal(KEY);

    f1.get(1, TimeUnit.SECONDS);
    f2.get(1, TimeUnit.SECONDS);
    f3.get(1, TimeUnit.SECONDS);

    assertTrue(f1.isDone());
    assertTrue(f2.isDone());
    assertTrue(f3.isDone());
  }

  @Test
  void testMultipleSubscriptionsInAnyOf() throws Exception {
    var f1 = map.subscribe(KEY_A);
    var f2 = map.subscribe(KEY_B);

    var anyOf = CompletableFuture.anyOf(f1, f2);
    assertFalse(anyOf.isDone());

    map.raiseSignal(KEY_B);

    anyOf.get(1, TimeUnit.SECONDS);
    assertTrue(f2.isDone());
    assertFalse(f1.isDone());
  }

  @Test
  void testSignalOnlyWakesMatchingKey() {
    var f1 = map.subscribe(KEY_A);
    var f2 = map.subscribe(KEY_B);

    map.raiseSignal(KEY_A);

    assertTrue(f1.isDone());
    assertFalse(f2.isDone());
  }

  @Test
  void testDifferentKeyPrefixesDoNotCollide() {
    // simulate how SystemDatabase.toStringKey differentiates type by prefix
    var eventSub = map.subscribe("e::wf-1::wf-1");
    var cancelSub = map.subscribe("c::wf-1");

    map.raiseSignal("c::wf-1");

    assertTrue(cancelSub.isDone());
    assertFalse(eventSub.isDone());
  }

  @Test
  void testSignalBeforeSubscribeDoesNotWake() {
    map.raiseSignal(KEY);

    var f = map.subscribe(KEY);
    assertFalse(f.isDone());

    map.raiseSignal(KEY);
    assertTrue(f.isDone());
  }

  @Test
  void testSignalIsOneShot() {
    var f1 = map.subscribe(KEY);
    map.raiseSignal(KEY);
    assertTrue(f1.isDone());

    var f2 = map.subscribe(KEY);
    assertFalse(f2.isDone());
  }

  // --- Subscription / close ---

  @Test
  void testCloseOneSubscriberDoesNotOrphanOthers() throws Exception {
    var sub1 = map.subscribe(KEY);
    var sub2 = map.subscribe(KEY);

    sub1.close();

    map.raiseSignal(KEY);
    sub2.get(1, TimeUnit.SECONDS);
    assertTrue(sub2.isDone());
  }

  @Test
  void testClosePreventsFutureFromBeingSignalled() throws Exception {
    var sub = map.subscribe(KEY);
    sub.close();
    map.raiseSignal(KEY);

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
    var f = map.subscribe(FOO);

    CompletableFuture.runAsync(
        () -> {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          map.raiseSignal(FOO);
        });

    assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) f::get);
  }

  @Test
  void testSignalFromMainAfterBackgroundSubscribes() throws Exception {
    var backgroundDone = new CompletableFuture<Void>();

    CompletableFuture.runAsync(
        () -> {
          var f = map.subscribe(FOO);
          try {
            backgroundDone.complete(f.get(500, TimeUnit.MILLISECONDS));
          } catch (Exception e) {
            backgroundDone.completeExceptionally(e);
          }
        });

    Thread.sleep(100);
    map.raiseSignal(FOO);

    assertTimeoutPreemptively(Duration.ofSeconds(1), (Executable) backgroundDone::get);
  }

  @Test
  void testMultipleSubscribersInSeparateThreads() throws Exception {
    var done1 = new CompletableFuture<Void>();
    var done2 = new CompletableFuture<Void>();
    var subscribed = new CountDownLatch(2);

    CompletableFuture.runAsync(
        () -> {
          try {
            var sub = map.subscribe(FOO);
            subscribed.countDown();
            done1.complete(sub.get(5, TimeUnit.SECONDS));
          } catch (Exception e) {
            done1.completeExceptionally(e);
          }
        });
    CompletableFuture.runAsync(
        () -> {
          try {
            var sub = map.subscribe(FOO);
            subscribed.countDown();
            done2.complete(sub.get(5, TimeUnit.SECONDS));
          } catch (Exception e) {
            done2.completeExceptionally(e);
          }
        });

    // Ensure both subscriptions are registered before signalling; the signal is one-shot,
    // so a subscriber that registers after raiseSignal would miss it and time out.
    assertTrue(subscribed.await(5, TimeUnit.SECONDS), "subscribers did not register in time");
    map.raiseSignal(FOO);

    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          done1.get();
          done2.get();
        });
  }

  @Test
  void testConcurrentSignalAndSubscribe() throws Exception {
    // Use a dedicated executor so signal dispatch isn't starved by an overloaded common pool.
    var executor = Executors.newCachedThreadPool();
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(10),
          () -> {
            for (int i = 0; i < 1000; i++) {
              var m = new SignalMap();
              var sub = m.subscribe(KEY);
              CompletableFuture.runAsync(() -> m.raiseSignal(KEY), executor);
              sub.get(1, TimeUnit.SECONDS);
            }
          });
    } finally {
      executor.shutdownNow();
    }
  }

  // --- awaitAny ---

  @Test
  void testAwaitAny_notifyFires() throws Exception {
    var onNotify = map.subscribe("e::wf-1::topic");
    var onCancelled = map.subscribe("c::wf-1");
    var onShutdown = never();

    map.raiseSignal("e::wf-1::topic");

    SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown);

    assertTrue(onNotify.isDone());
    assertFalse(onCancelled.isDone());
    assertFalse(onShutdown.isDone());
  }

  @Test
  void testAwaitAny_cancelledFires() throws Exception {
    var onNotify = map.subscribe("e::wf-1::topic");
    var onCancelled = map.subscribe("c::wf-1");
    var onShutdown = never();

    map.raiseSignal("c::wf-1");

    SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown);

    assertTrue(onCancelled.isDone());
    assertFalse(onNotify.isDone());
    assertFalse(onShutdown.isDone());
  }

  @Test
  void testAwaitAny_shutdownFires() throws Exception {
    var onNotify = map.subscribe("e::wf-1::topic");
    var onCancelled = never();
    var onShutdown = map.subscribe("__shutdown__");

    map.raiseSignal("__shutdown__");

    SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown);

    assertTrue(onShutdown.isDone());
    assertFalse(onNotify.isDone());
  }

  @Test
  void testAwaitAny_timeout() {
    var onNotify = map.subscribe("e::wf-1::topic");
    var onCancelled = never();
    var onShutdown = never();

    SignalMap.awaitAny(Duration.ofMillis(50), onNotify, onCancelled, onShutdown);

    assertFalse(onNotify.isDone());
    assertFalse(onCancelled.isDone());
    assertFalse(onShutdown.isDone());
  }

  // --- isDone checks after awaitAny ---

  @Test
  void testCheckIsDone_notifyFires() throws Exception {
    var onNotify = map.subscribe("e::wf-1::topic");
    var onCancelled = map.subscribe("c::wf-1");
    var onShutdown = never();

    map.raiseSignal("e::wf-1::topic");

    SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown);

    assertTrue(onNotify.isDone());
    assertFalse(onCancelled.isDone() || onShutdown.isDone());
  }

  @Test
  void testCheckIsDone_cancelledFires() throws Exception {
    var onNotify = map.subscribe("e::wf-1::topic");
    var onCancelled = map.subscribe("c::wf-1");
    var onShutdown = never();

    map.raiseSignal("c::wf-1");

    SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown);

    assertTrue(onCancelled.isDone());
    assertFalse(onNotify.isDone() || onShutdown.isDone());
  }

  @Test
  void testCheckIsDone_shutdownFires() throws Exception {
    var onNotify = map.subscribe("e::wf-1::topic");
    var onCancelled = never();
    var onShutdown = map.subscribe("__shutdown__");

    map.raiseSignal("__shutdown__");

    SignalMap.awaitAny(Duration.ofSeconds(1), onNotify, onCancelled, onShutdown);

    assertTrue(onShutdown.isDone());
    assertFalse(onNotify.isDone());
  }

  @Test
  void testCheckIsDone_timeout() {
    var onNotify = map.subscribe("e::wf-1::topic");
    var onCancelled = never();
    var onShutdown = never();

    SignalMap.awaitAny(Duration.ofMillis(50), onNotify, onCancelled, onShutdown);

    assertFalse(onNotify.isDone());
    assertFalse(onCancelled.isDone() || onShutdown.isDone());
  }
}
