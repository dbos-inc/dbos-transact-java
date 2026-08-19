package dev.dbos.transact.database;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase.NotificationSource;
import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.database.signal.SignalMap;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Both directions of the PostgreSQL LISTEN/NOTIFY transport between processes sharing a system
 * database: notifications observed on the wire are turned into local wake-ups, and wake-ups this
 * process writes are pushed out to everyone else.
 *
 * <p>The two halves are one class because they are one decision. Either this database carries
 * notifications, in which case a process both listens and pushes, or it does not and neither
 * happens — see {@code NullNotificationSource}. Their machinery differs, though: listening holds a
 * dedicated connection parked in a reconnect loop, while pushing borrows a pooled one for a moment
 * on a timer.
 *
 * <p>Pushes are batched. A transaction that sends a NOTIFY takes a global lock, so emitting one per
 * row written serializes writes against every other notifying transaction in the database; queueing
 * the payload and flushing on an interval bounds the added wake-up latency at that interval and
 * caps the rate of notifying commits however fast the writes arrive.
 *
 * <p>Not everything is pushed from here. The notifications channel keeps a database trigger, which
 * fires inside the writing transaction so a {@code recv} is never woken before the row it would
 * read has committed, and which works from a process running no flusher at all.
 */
class ListenNotifySource implements NotificationSource {

  private static final Logger logger = LoggerFactory.getLogger(ListenNotifySource.class);

  /** Bounds how long the final flush may hold up shutdown. */
  private static final long SHUTDOWN_TIMEOUT_SEC = 5;

  private final DbContext ctx;
  private final Duration coalesceInterval;
  private final SignalMap signals;

  private final Object lock = new Object();
  private final Map<String, Set<String>> pending = new HashMap<>();

  private final AtomicReference<Thread> listenerThread = new AtomicReference<>(null);
  private final AtomicReference<ScheduledExecutorService> flusher = new AtomicReference<>(null);

  ListenNotifySource(DbContext ctx, Duration coalesceInterval, SignalMap signals) {
    this.ctx = ctx;
    this.coalesceInterval =
        coalesceInterval == null
            ? DBOSConfig.DEFAULT_NOTIFICATION_COALESCE_INTERVAL
            : coalesceInterval;
    this.signals = signals;
  }

  @Override
  public void start() {
    var listener = new Thread(this::listen, "NotificationListener");
    listener.setDaemon(true);
    if (listenerThread.compareAndSet(null, listener)) {
      listener.start();
      logger.debug("Notification listener started");
    }

    var executor = Executors.newSingleThreadScheduledExecutor();
    if (flusher.compareAndSet(null, executor)) {
      var ms = coalesceInterval.toMillis();
      // Fixed delay, not fixed rate: a slow flush must not queue more up behind it.
      executor.scheduleWithFixedDelay(this::flushQuietly, ms, ms, TimeUnit.MILLISECONDS);
      logger.debug("Notification flusher started, every {}ms", ms);
    } else {
      executor.shutdown();
    }
  }

  @Override
  public void close() {
    var executor = flusher.getAndSet(null);
    if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    // Writes made just before shutdown still wake waiters elsewhere promptly.
    flush();

    var listener = listenerThread.getAndSet(null);
    if (listener != null) {
      listener.interrupt();
      try {
        listener.join(SHUTDOWN_TIMEOUT_SEC * 1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    logger.debug("Notification transport stopped");
  }

  @Override
  public boolean isRunning() {
    return listenerThread.get() != null;
  }

  /**
   * Queue a wake-up for the processes listening on {@code channel}. Call only after the write has
   * committed, or a woken waiter may re-read before the row is visible and go back to sleep until
   * its next poll.
   */
  @Override
  public void push(String channel, String payload) {
    synchronized (lock) {
      // A set, so repeated writes to one key between flushes cost one wake-up, not one each.
      pending.computeIfAbsent(channel, c -> new LinkedHashSet<>()).add(payload);
    }
  }

  /**
   * An exception escaping a task cancels every later run of a scheduleWithFixedDelay, silently and
   * permanently. flush already contains its own failures, so this is the backstop for the rest.
   */
  private void flushQuietly() {
    try {
      flush();
    } catch (RuntimeException e) {
      logger.warn("Notification flush failed; readers fall back to polling", e);
    }
  }

  /** Emit one notifying transaction per channel for everything queued since the last flush. */
  void flush() {
    Map<String, Set<String>> batches;
    synchronized (lock) {
      if (pending.isEmpty()) {
        return;
      }
      batches = new HashMap<>(pending);
      pending.clear();
    }

    for (var entry : batches.entrySet()) {
      var channel = entry.getKey();
      var payloads = new ArrayList<>(entry.getValue());
      if (payloads.isEmpty()) {
        continue;
      }
      try {
        pushBatch(channel, payloads);
      } catch (SQLException | RuntimeException e) {
        // Drop the batch rather than requeue it: a payload over pg_notify's 8000-byte limit would
        // otherwise stall the flusher forever, and every waiter still makes progress on its poll.
        logger.warn(
            "Failed to push {} notification(s) on {}; readers fall back to polling",
            payloads.size(),
            channel,
            e);
      }
    }
  }

  private void pushBatch(String channel, List<String> payloads) throws SQLException {
    // One statement, one transaction: one round trip and one acquisition of the async-notify queue
    // lock, however many payloads the batch holds.
    var sql = "SELECT pg_notify(?, p) FROM unnest(?::text[]) AS p";
    try (var conn = ctx.getConnection();
        var ps = conn.prepareStatement(sql)) {
      ps.setString(1, channel);
      ps.setArray(2, conn.createArrayOf("text", payloads.toArray()));
      ps.execute();
    }
  }

  private void listen() {
    while (listenerThread.get() == Thread.currentThread()) {
      Connection notificationConnection = null;

      try {
        notificationConnection = ctx.getConnection();
        notificationConnection.setAutoCommit(true);

        // Cast to PostgreSQL connection for notification support
        PGConnection pgConnection = notificationConnection.unwrap(PGConnection.class);

        try (Statement stmt = notificationConnection.createStatement()) {
          stmt.execute("LISTEN " + SignalKey.NOTIFICATIONS_CHANNEL);
          stmt.execute("LISTEN " + SignalKey.WORKFLOW_EVENTS_CHANNEL);
          stmt.execute("LISTEN " + SignalKey.STREAMS_CHANNEL);
        }

        // Anything notified while this process had no connection is gone: NOTIFY is not queued
        // for absent listeners. Make every waiter look again, so a row written during the outage
        // is found now rather than at a re-check that may be further off than its timeout.
        signals.raiseAll();
        logger.debug("Listening for PostgreSQL notifications");

        while (listenerThread.get() == Thread.currentThread()) {
          // Check for notifications with a one second timeout
          PGNotification[] notifications = pgConnection.getNotifications(1000);

          if (notifications != null) {
            for (PGNotification notification : notifications) {
              String channel = notification.getName();
              String payload = notification.getParameter();

              logger.debug("Received notification on channel: {}, payload: {}", channel, payload);

              if (null == channel) {
                logger.error("Received notification with null channel. Payload: {}", payload);
              } else
                try {
                  signals.raiseSignal(SignalKey.signalFor(channel, payload));
                } catch (Exception e) {
                  logger.error(
                      "Error raising signal for channel: {}, payload: {}", channel, payload, e);
                }
            }
          }
        }
      } catch (Exception e) {
        if (listenerThread.get() == Thread.currentThread()) {
          logger.warn("Notification listener error: {}", e.getMessage());
          try {
            Thread.sleep(1000); // Wait before retrying
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
          // Loop will try to reconnect and restart the listener
        }
      } finally {
        try {
          if (notificationConnection != null) {
            notificationConnection.close();
          }
        } catch (SQLException e) {
          logger.error("Error closing notification connection", e);
        }
      }
    }
    logger.debug("Notification listener thread exiting");
  }
}
