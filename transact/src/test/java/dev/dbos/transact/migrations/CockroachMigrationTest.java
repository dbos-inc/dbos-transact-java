package dev.dbos.transact.migrations;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.PgContainer;

import java.sql.Connection;

import org.junit.jupiter.api.Test;

/**
 * Tests that LISTEN/NOTIFY functions and triggers are created on PG and omitted on CRDB. Each test
 * spins up its own container so these tests are always tied to the specified DB type regardless of
 * how PgContainer is configured for the rest of the test suite.
 */
class CockroachMigrationTest {

  @Test
  void testPg_isCockroachReturnsFalse() throws Exception {
    try (var pg = PgContainer.getPG()) {
      pg.start();
      try (var ds =
          SystemDatabase.createDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
        assertFalse(SystemDatabase.isCockroach(ds));
        try (Connection conn = ds.getConnection()) {
          assertFalse(SystemDatabase.isCockroach(conn));
        }
      }
    }
  }

  @Test
  void testCrdb_isCockroachReturnsTrue() throws Exception {
    try (var crdb = PgContainer.getCRDB()) {
      crdb.start();
      try (var ds =
          SystemDatabase.createDataSource(
              crdb.getJdbcUrl(), crdb.getUsername(), crdb.getPassword())) {
        assertTrue(SystemDatabase.isCockroach(ds));
        try (Connection conn = ds.getConnection()) {
          assertTrue(SystemDatabase.isCockroach(conn));
        }
      }
    }
  }

  @Test
  void testPg_notifyFunctionsAndTriggersPresent() throws Exception {
    try (var pg = PgContainer.getPG()) {
      pg.start();
      try (var ds =
          SystemDatabase.createDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
        var config = DBOSConfig.defaults("migration-notify-test").withDataSource(ds);
        MigrationManager.runMigrations(config);

        try (Connection conn = ds.getConnection()) {
          var meta = conn.getMetaData();
          MigrationManagerTest.assertFunctionExists(meta, "notifications_function");
          MigrationManagerTest.assertFunctionExists(meta, "workflow_events_function");
          MigrationManagerTest.assertTriggerExists(conn, "dbos_notifications_trigger");
          MigrationManagerTest.assertTriggerExists(conn, "dbos_workflow_events_trigger");
        }
      }
    }
  }

  @Test
  void testPg_noListenNotify_notifyFunctionsAndTriggersAbsent() throws Exception {
    try (var pg = PgContainer.getPG()) {
      pg.start();
      try (var ds =
          SystemDatabase.createDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
        var config =
            DBOSConfig.defaults("migration-notify-test")
                .withDataSource(ds)
                .withUseListenNotify(false);
        MigrationManager.runMigrations(config);

        try (Connection conn = ds.getConnection()) {
          var meta = conn.getMetaData();
          MigrationManagerTest.assertFunctionAbsent(conn, "notifications_function");
          MigrationManagerTest.assertFunctionAbsent(conn, "workflow_events_function");
          MigrationManagerTest.assertTriggerAbsent(conn, "dbos_notifications_trigger");
          MigrationManagerTest.assertTriggerAbsent(conn, "dbos_workflow_events_trigger");
          MigrationManagerTest.assertFunctionExists(meta, "enqueue_workflow");
          MigrationManagerTest.assertFunctionExists(meta, "send_message");
        }
      }
    }
  }

  @Test
  void testCrdb_notifyFunctionsAndTriggersAbsent() throws Exception {
    try (var crdb = PgContainer.getCRDB()) {
      crdb.start();
      try (var ds =
          SystemDatabase.createDataSource(
              crdb.getJdbcUrl(), crdb.getUsername(), crdb.getPassword())) {
        // useListenNotify=true intentionally — MigrationManager must override it to false for CRDB
        var config = DBOSConfig.defaults("migration-notify-test").withDataSource(ds);
        MigrationManager.runMigrations(config);

        try (Connection conn = ds.getConnection()) {
          MigrationManagerTest.assertFunctionAbsent(conn, "notifications_function");
          MigrationManagerTest.assertFunctionAbsent(conn, "workflow_events_function");
          MigrationManagerTest.assertTriggerAbsent(conn, "dbos_notifications_trigger");
          MigrationManagerTest.assertTriggerAbsent(conn, "dbos_workflow_events_trigger");
        }
      }
    }
  }
}
