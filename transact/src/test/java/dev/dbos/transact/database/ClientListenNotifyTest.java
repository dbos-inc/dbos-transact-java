package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A client polls for getEvent and readStream unless it is asked for a listener, and when it is
 * asked, the listener actually runs: it was once built and left unstarted, so opting in bought
 * nothing but every wait still re-checked on the slow interval.
 */
class ClientListenNotifyTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  DBOSConfig dbosConfig;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    dataSource = pgContainer.dataSource();
  }

  @Test
  void dataSourceClientHonoursListenNotifyDisabled() {
    try (var client = new DBOSClient(dataSource, null, null, false)) {
      assertFalse(DBOSTestAccess.getSystemDatabase(client).hasNotificationListener());
    }
  }

  @Test
  void dataSourceClientPollsByDefault() {
    try (var client = new DBOSClient(dataSource)) {
      assertFalse(DBOSTestAccess.getSystemDatabase(client).hasNotificationListener());
    }
  }

  @Test
  void urlClientPollsByDefault() {
    try (var client =
        new DBOSClient(pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password())) {
      assertFalse(DBOSTestAccess.getSystemDatabase(client).hasNotificationListener());
    }
  }

  @Test
  void dataSourceClientRunsTheListenerWhenAskedTo() {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "LISTEN/NOTIFY is PostgreSQL-only");

    try (var client = new DBOSClient(dataSource, null, null, true)) {
      var sysdb = DBOSTestAccess.getSystemDatabase(client);
      assertTrue(sysdb.hasNotificationListener());
      assertTrue(
          sysdb.isNotificationListenerRunning(),
          "Opting in must start the listener, not just construct one");
    }
  }

  @Test
  void urlClientRunsTheListenerWhenAskedTo() {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "LISTEN/NOTIFY is PostgreSQL-only");

    try (var client =
        new DBOSClient(
            pgContainer.jdbcUrl(),
            pgContainer.username(),
            pgContainer.password(),
            null,
            null,
            true)) {
      var sysdb = DBOSTestAccess.getSystemDatabase(client);
      assertTrue(sysdb.hasNotificationListener());
      assertTrue(
          sysdb.isNotificationListenerRunning(),
          "Opting in must start the listener, not just construct one");
    }
  }

  @Test
  void urlClientHonoursListenNotifyDisabled() {
    try (var client =
        new DBOSClient(
            pgContainer.jdbcUrl(),
            pgContainer.username(),
            pgContainer.password(),
            null,
            null,
            false)) {
      assertFalse(DBOSTestAccess.getSystemDatabase(client).hasNotificationListener());
    }
  }
}
