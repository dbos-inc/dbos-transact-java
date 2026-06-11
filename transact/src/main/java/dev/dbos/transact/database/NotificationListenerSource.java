package dev.dbos.transact.database;

import dev.dbos.transact.database.SystemDatabase.NotificationSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NotificationListenerSource implements NotificationSource {

  private static final Logger logger = LoggerFactory.getLogger(NotificationListenerSource.class);

  private final DataSource dataSource;
  private final Consumer<String> raiseSignal;
  private final AtomicReference<Thread> notificationListenerThread = new AtomicReference<>(null);

  public NotificationListenerSource(DataSource dataSource, Consumer<String> raiseSignal) {
    this.dataSource = dataSource;
    this.raiseSignal = raiseSignal;
  }

  @Override
  public void start() {
    Thread t = new Thread(this::notificationListener, "NotificationListener");
    t.setDaemon(true);
    if (notificationListenerThread.compareAndSet(null, t)) {
      t.start();
      logger.debug("Notification listener started");
    }
  }

  @Override
  public void close() {
    Thread t = notificationListenerThread.getAndSet(null);
    if (t != null) {
      t.interrupt();
      try {
        t.join(5000); // Wait up to 5 seconds
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

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
          stmt.execute("LISTEN dbos_streams_channel");
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
                  case "dbos_notifications_channel" -> raiseSignal.accept("m::" + payload);
                  case "dbos_workflow_events_channel" -> raiseSignal.accept("e::" + payload);
                  case "dbos_streams_channel" -> raiseSignal.accept("s::" + payload);
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
}
