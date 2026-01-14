package dev.dbos.transact.migrations;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.invocation.HawkService;
import dev.dbos.transact.invocation.HawkServiceImpl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class BackCompatTest {
  private DBOSConfig config;

  @BeforeEach
  void onetimeSetup() throws Exception {

    config =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys");

    var pair = MigrationManager.extractDbAndPostgresUrl(config.databaseUrl());
    var dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", pair.database());
    try (var conn = DriverManager.getConnection(pair.url(), config.dbUser(), config.dbPassword());
        var stmt = conn.createStatement()) {
      stmt.execute(dropDbSql);
    }
  }

  @Test
  void testInitialRun() throws Exception {

    runDbos();

    try (var dataSource = SystemDatabase.createDataSource(config);
        Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();

      MigrationManagerTest.assertTableExists(metaData, "operation_outputs");
      MigrationManagerTest.assertTableExists(metaData, "workflow_status");
      MigrationManagerTest.assertTableExists(metaData, "notifications");
      MigrationManagerTest.assertTableExists(metaData, "workflow_events");
    }
  }

  @Test
  void testWayFutureVersion() throws Exception {
    testInitialRun();

    try (var dataSource = SystemDatabase.createDataSource(config);
        var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.executeUpdate("UPDATE \"dbos\".\"dbos_migrations\" SET \"version\" = 10000;");
    }

    runDbos();
  }

  @Test
  void testIdempotence() throws Exception {
    testWayFutureVersion();

    try (var dataSource = SystemDatabase.createDataSource(config);
        var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.executeUpdate("UPDATE \"dbos\".\"dbos_migrations\" SET \"version\" = 0;");
    }

    runDbos();
  }

  void runDbos() {
    try {
      DBOS.reinitialize(config);
      var impl = new HawkServiceImpl();
      var proxy = DBOS.registerWorkflows(HawkService.class, impl);
      impl.setProxy(proxy);

      DBOS.launch();

      var localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
      var result = proxy.simpleWorkflow();
      assertEquals(localDate, result);
    } finally {
      DBOS.shutdown();
    }
  }
}
