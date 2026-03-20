package dev.dbos.transact.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationService {

  public static class LockConditionPair {
    public final ReentrantLock lock = new ReentrantLock();
    public final Condition condition = lock.newCondition();
  }

  private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);

  private final Map<String, LockConditionPair> notificationsMap = new ConcurrentHashMap<>();
  private final AtomicReference<Thread> notificationListenerThread = new AtomicReference<>(null);
  private final DataSource dataSource;

  public NotificationService(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public boolean registerNotificationCondition(String key, LockConditionPair pair) {
    return notificationsMap.putIfAbsent(key, pair) == null;
  }

  public LockConditionPair getOrCreateNotificationCondition(String key) {
    return notificationsMap.computeIfAbsent(key, k -> new LockConditionPair());
  }

  public void unregisterNotificationCondition(String key) {
    notificationsMap.remove(key);
  }

  public void start() {
    Thread t = new Thread(this::notificationListener, "NotificationListener");
    t.setDaemon(true);
    if (notificationListenerThread.compareAndSet(null, t)) {
      t.start();
      logger.debug("Notification listener started");
    }
  }

  public void stop() {
    Thread t = notificationListenerThread.getAndSet(null);
    if (t != null) {
      t.interrupt();
      try {
        t.join(5000); // Wait up to 5 seconds
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    notificationsMap.clear();
    logger.debug("Notification listener stopped");
  }

  private void notificationListener() {
    while (notificationListenerThread.get() == Thread.currentThread()) {
      Connection notificationConnection = null;

      try {
        notificationConnection = dataSource.getConnection();
        notificationConnection.setAutoCommit(true);

        // Cast to PostgreSQL connection for notification support
        PGConnection pgConnection = notificationConnection.unwrap(PGConnection.class);

        // Listen to notification channels
        try (Statement stmt = notificationConnection.createStatement()) {
          stmt.execute("LISTEN dbos_notifications_channel");
          stmt.execute("LISTEN dbos_workflow_events_channel");
        }

        logger.debug("Listening for PostgreSQL notifications");

        while (notificationListenerThread.get() == Thread.currentThread()) {
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
                switch (channel) {
                  case "dbos_notifications_channel" -> handleNotification(payload, "notifications");
                  case "dbos_workflow_events_channel" ->
                      handleNotification(payload, "workflow_events");
                  default -> logger.error("Unknown NOTIFY channel: {}", channel);
                }
            }
          }
        }

      } catch (Exception e) {
        if (notificationListenerThread.get() == Thread.currentThread()) {
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

  private void handleNotification(String payload, String mapType) {

    logger.debug("Received notification for {}", payload);

    if (payload != null && !payload.isEmpty()) {
      LockConditionPair pair = notificationsMap.get(payload);
      if (pair != null) {
        pair.lock.lock();
        try {
          pair.condition.signalAll();
        } finally {
          pair.lock.unlock();
        }
        logger.debug("Signaled {} condition for {}", mapType, payload);
      } else {
        logger.debug("ConditionMap has no entry for {}", payload);
      }
      // If no condition found, we simply ignore the notification
    }
  }
}
