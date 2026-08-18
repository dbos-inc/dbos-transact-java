package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.SerializationStrategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;

/**
 * The push half of the LISTEN/NOTIFY transport: the stream and workflow-event wake-ups this process
 * writes reach the processes listening for them, batched, now that no database trigger sends them
 * per row.
 */
class ListenNotifySourceTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose HikariDataSource dataSource;
  DbContext ctx;

  @BeforeEach
  void beforeEach() {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "LISTEN/NOTIFY is PostgreSQL-only");
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    dataSource = pgContainer.dataSource();
    ctx = new DbContext(dataSource, "dbos", null, () -> false, null, new PollingLimiter(0));
  }

  @Test
  void collapsesRepeatedPayloadsAndPushesTheRestInOneBatch() throws Exception {
    try (var listener = listenOn(SignalKey.STREAMS_CHANNEL)) {
      var source = new ListenNotifySource(ctx, Duration.ofMillis(10), signal -> {});

      // Three writes, two distinct keys: a reader only needs to be told to look again once per key.
      source.push(SignalKey.STREAMS_CHANNEL, "wf::key");
      source.push(SignalKey.STREAMS_CHANNEL, "wf::key");
      source.push(SignalKey.STREAMS_CHANNEL, "wf::other");
      source.flush();

      assertEquals(Set.of("wf::key", "wf::other"), drain(listener));
    }
  }

  @Test
  void keepsChannelsApart() throws Exception {
    try (var listener = listenOn(SignalKey.STREAMS_CHANNEL)) {
      var source = new ListenNotifySource(ctx, Duration.ofMillis(10), signal -> {});
      source.push(SignalKey.WORKFLOW_EVENTS_CHANNEL, "wf::event");
      source.push(SignalKey.STREAMS_CHANNEL, "wf::stream");
      source.flush();

      assertEquals(Set.of("wf::stream"), drain(listener));
    }
  }

  @Test
  void flushingTwiceDoesNotResendABatch() throws Exception {
    try (var listener = listenOn(SignalKey.STREAMS_CHANNEL)) {
      var source = new ListenNotifySource(ctx, Duration.ofMillis(10), signal -> {});
      source.push(SignalKey.STREAMS_CHANNEL, "wf::key");
      source.flush();
      assertEquals(Set.of("wf::key"), drain(listener));

      source.flush();
      assertEquals(Set.of(), drain(listener));
    }
  }

  @Test
  void pushesNothingWithoutListenNotify() throws Exception {
    // The transport is absent entirely, so a write reaches nobody over the wire. Waiters in the
    // writing process are still woken directly -- see aLocalWaiterIsWokenWithoutARoundTrip.
    try (var listener = listenOn(SignalKey.STREAMS_CHANNEL)) {
      var config = pgContainer.dbosConfig().withUseListenNotify(false);
      try (var sysdb = SystemDatabase.create(config)) {
        sysdb.start();
        sysdb.initWorkflowStatus(
            WorkflowStatusInternalBuilder.create("wf-no-listen-notify").build(), 5, false, false);
        sysdb.writeStreamFromWorkflow("wf-no-listen-notify", 1, "key1", "v", "portable_json");
      }

      assertEquals(Set.of(), drain(listener));
    }
  }

  @Test
  void startedSourceFlushesOnItsOwnInterval() throws Exception {
    try (var listener = listenOn(SignalKey.STREAMS_CHANNEL)) {
      var source = new ListenNotifySource(ctx, Duration.ofMillis(10), signal -> {});
      source.start();
      try {
        source.push(SignalKey.STREAMS_CHANNEL, "wf::key");
        assertEquals(Set.of("wf::key"), drain(listener));
      } finally {
        source.close();
      }
    }
  }

  @Test
  void closeFlushesWhatWasQueuedJustBeforeShutdown() throws Exception {
    try (var listener = listenOn(SignalKey.STREAMS_CHANNEL)) {
      var source = new ListenNotifySource(ctx, Duration.ofSeconds(30), signal -> {});
      source.start();
      source.push(SignalKey.STREAMS_CHANNEL, "wf::key");
      // The interval has not elapsed and never will within this test: only the final flush can
      // deliver this, which is what a write immediately before shutdown depends on.
      source.close();

      assertEquals(Set.of("wf::key"), drain(listener));
    }
  }

  @Test
  void aLocalWaiterIsWokenWithoutARoundTrip() throws Exception {
    // No LISTEN/NOTIFY at all, so nothing is pushed and nothing is received: the waiter can only be
    // woken by the writing process signalling it directly, or by its own one-second poll.
    var config = pgContainer.dbosConfig().withUseListenNotify(false);
    try (var sysdb = SystemDatabase.create(config)) {
      sysdb.start();
      // setEvent records a step against the writing workflow, so it has to exist.
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create("wf1").build(), 5, false, false);

      var waiting = new CountDownLatch(1);
      var result = new CompletableFuture<Object>();
      var reader =
          new Thread(
              () -> {
                waiting.countDown();
                result.complete(sysdb.getEvent("wf1", "k", Duration.ofSeconds(10), null));
              });
      reader.setDaemon(true);
      reader.start();

      // Let the reader subscribe and take its first look, so what follows can only be a wake-up.
      assertTrue(waiting.await(5, TimeUnit.SECONDS));
      Thread.sleep(300);

      var start = System.nanoTime();
      sysdb.setEvent("wf1", 0, "k", "v", true, SerializationStrategy.DEFAULT.formatName());
      assertEquals("v", result.get(5, TimeUnit.SECONDS));
      var elapsedMs = (System.nanoTime() - start) / 1_000_000;

      // The poll fallback would take up to a second from here; a direct wake-up is immediate.
      assertTrue(
          elapsedMs < 500,
          "waiter took %dms, so it was polled awake, not signalled".formatted(elapsedMs));
    }
  }

  @Test
  void rejectsACoalesceIntervalBelowOneMillisecond() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DBOSConfig.defaults("listen-notify-test")
                    .withNotificationCoalesceInterval(Duration.ZERO));
    assertTrue(ex.getMessage().contains("notificationCoalesceInterval"));

    // Null is the documented way to ask for the default.
    assertNull(
        DBOSConfig.defaults("listen-notify-test")
            .withNotificationCoalesceInterval(null)
            .notificationCoalesceInterval());
  }

  private Connection listenOn(String channel) throws Exception {
    var conn =
        DriverManager.getConnection(
            pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password());
    conn.setAutoCommit(true);
    try (var stmt = conn.createStatement()) {
      stmt.execute("LISTEN " + channel);
    }
    return conn;
  }

  /** Payloads seen on the listener within a bounded wait; empty if none arrive. */
  private Set<String> drain(Connection listener) throws Exception {
    var notifications = listener.unwrap(PGConnection.class).getNotifications(2000);
    if (notifications == null) {
      return Set.of();
    }
    return Arrays.stream(notifications)
        .map(n -> n.getParameter())
        .collect(Collectors.toCollection(HashSet::new));
  }
}
