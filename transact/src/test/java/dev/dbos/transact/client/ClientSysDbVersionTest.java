package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;

import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A client never migrates, so it is the caller's job to point it at a schema this SDK can read. It
 * checks that on construction rather than failing later on a raw "relation does not exist".
 *
 * <p>The suite-wide two-minute timeout does not fit this class. The tests that need a schema to
 * check migrate a fresh container from empty, which on CockroachDB is over a hundred online schema
 * changes: 20-40s on a CI runner, but two to three minutes on a slow one (#529). Raised rather than
 * removed, so a genuine hang still fails rather than hanging CI.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class ClientSysDbVersionTest {

  @AutoClose final PgContainer pgContainer = PgContainer.createFresh();
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void setup() {
    dataSource = pgContainer.dataSource();
  }

  @Test
  void rejectsUnversionedSchema() {
    // The database exists but nothing has been migrated into it, so dbos_migrations is absent.
    pgContainer.createDatabase();

    var e = assertThrows(IllegalStateException.class, () -> pgContainer.dbosClient());
    assertTrue(
        e.getMessage().contains("dbos_migrations"),
        "Expected the message to name the missing table, got: " + e.getMessage());
  }

  @Test
  void rejectsTooOldSchema() throws Exception {
    MigrationManager.runMigrations(pgContainer.dbosConfig());

    var tooOld = MigrationManager.MINIMUM_SYSDB_VERSION - 1;
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.executeUpdate(
          "UPDATE \"%s\".dbos_migrations SET version = %d".formatted(Constants.DB_SCHEMA, tooOld));
    }

    var e = assertThrows(IllegalStateException.class, () -> pgContainer.dbosClient());
    assertTrue(
        e.getMessage().contains(Integer.toString(tooOld))
            && e.getMessage().contains(Integer.toString(MigrationManager.MINIMUM_SYSDB_VERSION)),
        "Expected the message to report both versions, got: " + e.getMessage());
  }

  @Test
  void acceptsMigratedSchema() {
    MigrationManager.runMigrations(pgContainer.dbosConfig());

    assertDoesNotThrow(
        () -> {
          try (var client = pgContainer.dbosClient()) {}
        });
  }

  @Test
  void validatesDataSourceClients() {
    MigrationManager.runMigrations(pgContainer.dbosConfig());

    assertDoesNotThrow(
        () -> {
          try (var client = new DBOSClient(dataSource)) {}
        });
  }

  @Test
  void rejectsUnversionedSchemaOnDataSourceClients() {
    pgContainer.createDatabase();

    var e = assertThrows(IllegalStateException.class, () -> new DBOSClient(dataSource));
    assertTrue(
        e.getMessage().contains("dbos_migrations"),
        "Expected the message to name the missing table, got: " + e.getMessage());
  }
}
