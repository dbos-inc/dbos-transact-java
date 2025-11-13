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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BackCompatTest {
  private static DBOSConfig config;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    config =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);

    var pair = MigrationManager.extractDbAndPostgresUrl(config.databaseUrl());
    var dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", pair.database());
    try (var conn = DriverManager.getConnection(pair.url(), config.dbUser(), config.dbPassword());
        var stmt = conn.createStatement()) {
      stmt.execute(dropDbSql);
    }
  }

  @Test
  @Order(1)
  void testInitialRun() throws Exception {

    runDbos();

    var dataSource = SystemDatabase.createDataSource(config);
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();

      MigrationManagerTest.assertTableExists(metaData, "operation_outputs");
      MigrationManagerTest.assertTableExists(metaData, "workflow_status");
      MigrationManagerTest.assertTableExists(metaData, "notifications");
      MigrationManagerTest.assertTableExists(metaData, "workflow_events");
    }
  }

  @Test
  @Order(2)
  void testWayFutureVersion() throws Exception {
    var dataSource = SystemDatabase.createDataSource(config);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.executeUpdate("UPDATE \"dbos\".\"dbos_migrations\" SET \"version\" = 10000;");
    }

    runDbos();
  }

  @Test
  @Order(3)
  void testIdempotence() throws Exception {
    var dataSource = SystemDatabase.createDataSource(config);
    try (var conn = dataSource.getConnection();
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
