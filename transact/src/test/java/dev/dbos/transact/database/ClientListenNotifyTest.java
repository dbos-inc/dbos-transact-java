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
 * A client built from a data source used to start a listener whatever the caller asked for, so an
 * application that migrated with LISTEN/NOTIFY off got a listener with no triggers to hear: every
 * recv and getEvent silently fell back to the slow re-check.
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
  void dataSourceClientListensByDefault() {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "LISTEN/NOTIFY is PostgreSQL-only");

    try (var client = new DBOSClient(dataSource)) {
      assertTrue(DBOSTestAccess.getSystemDatabase(client).hasNotificationListener());
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
