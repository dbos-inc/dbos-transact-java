package dev.dbos.transact.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

  Logger logger = LoggerFactory.getLogger(NotificationService.class);

  private final Map<String, LockConditionPair> notificationsMap = new ConcurrentHashMap<>();
  private volatile boolean running = false;
  private Thread notificationListenerThread;
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
    if (!running) {
      running = true;
      notificationListenerThread = new Thread(this::notificationListener, "NotificationListener");
      notificationListenerThread.setDaemon(true);
      notificationListenerThread.start();
      logger.info("Notification listener started");
    }
  }

  public void stop() {
    running = false;
    if (notificationListenerThread != null) {
      notificationListenerThread.interrupt();
      try {
        notificationListenerThread.join(5000); // Wait up to 5 seconds
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    notificationsMap.clear();
    logger.info("Notification listener stopped");
  }

  private void notificationListener() {
    while (running) {
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

        while (running) {
          // Check for notifications with a timeout
          PGNotification[] notifications = pgConnection.getNotifications(1000); // 1
          // second
          // timeout

          if (notifications != null) {
            for (PGNotification notification : notifications) {
              String channel = notification.getName();
              String payload = notification.getParameter();

              logger.debug("Received notification on channel: {}, payload: {}", channel, payload);

              if ("dbos_notifications_channel".equals(channel)) {
                handleNotification(payload, "notifications");
              } else if ("dbos_workflow_events_channel".equals(channel)) {
                handleNotification(payload, "workflow_events");
              } else {
                logger.error("Unknown channel: {}", channel);
              }
            }
          }
        }

      } catch (Exception e) {
        if (running) {
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
        logger.warn("ConditionMap has no entry for {}", payload);
      }
      // If no condition found, we simply ignore the notification
    }
  }
}
