package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;
import dev.dbos.transact.workflow.WorkflowState;

import java.net.URI;
import java.util.UUID;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

interface ConfigTestService {
  String exampleWorklow();

  int addTwoPlusTwo();
}

@WorkflowClassName("ConfigTestService")
class ConfigTestServiceImpl implements ConfigTestService {
  @Override
  @Workflow
  public String exampleWorklow() {
    return String.valueOf(System.currentTimeMillis());
  }

  @Override
  @Workflow
  public int addTwoPlusTwo() {
    return 2 + 2;
  }

  public String question() {
    return "What do you get if you multiply six by nine?";
  }
}

@WorkflowClassName("ConfigTestService")
class ConfigTestServiceImplAgain implements ConfigTestService {
  @Override
  @Workflow
  public String exampleWorklow() {
    return String.valueOf(System.currentTimeMillis());
  }

  @Override
  @Workflow
  public int addTwoPlusTwo() {
    return 2 + 2;
  }

  public String answer() {
    return "42";
  }
}

@WorkflowClassName("ConfigTestService")
class ConfigTestServiceImplDifferentCode implements ConfigTestService {
  @Override
  @Workflow
  public String exampleWorklow() {
    return "%s".formatted(System.currentTimeMillis());
  }

  @Override
  @Workflow
  public int addTwoPlusTwo() {
    return 2 + 2;
  }
}

@WorkflowClassName("ConfigTestServiceFoo")
class ConfigTestServiceImplDifferentName implements ConfigTestService {
  @Override
  @Workflow
  public String exampleWorklow() {
    return String.valueOf(System.currentTimeMillis());
  }

  @Override
  @Workflow
  public int addTwoPlusTwo() {
    return 2 + 2;
  }
}

public class ConfigTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @Test
  public void localExecutorId() throws Exception {
    var config = pgContainer.dbosConfig();
    var dbos = new DBOS(config);

    try {
      dbos.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
      assertEquals("local", dbosExecutor.executorId());
      assertEquals("", dbosExecutor.appId());
    } finally {
      dbos.shutdown();
    }
  }

  @Test
  public void conductorExecutorId() throws Exception {
    var config = pgContainer.dbosConfig().withConductorKey("test-conductor-key");
    var dbos = new DBOS(config);

    try {
      dbos.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
      assertNotNull(dbosExecutor.executorId());
      assertDoesNotThrow(() -> UUID.fromString(dbosExecutor.executorId()));
      assertEquals("", dbosExecutor.appId());
    } finally {
      dbos.shutdown();
    }
  }

  @Test
  public void cantSetExecutorIdWhenUsingConductor() throws Exception {
    var config =
        pgContainer
            .dbosConfig()
            .withConductorKey("test-conductor-key")
            .withExecutorId("test-executor-id");

    try (var dbos = new DBOS(config)) {
      assertThrows(IllegalArgumentException.class, () -> dbos.launch());
    }
  }

  @Test
  public void cantSetEmptyConfigFields() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> DBOSConfig.defaults(null));
    assertThrows(IllegalArgumentException.class, () -> DBOSConfig.defaults(""));

    final var config = DBOSConfig.defaults("app-name");
    assertThrows(IllegalArgumentException.class, () -> config.withAppName(""));
    assertThrows(IllegalArgumentException.class, () -> config.withAppName(null));

    assertThrows(IllegalArgumentException.class, () -> config.withConductorKey(""));
    assertDoesNotThrow(() -> config.withConductorKey(null));
    assertThrows(IllegalArgumentException.class, () -> config.withConductorDomain(""));
    assertDoesNotThrow(() -> config.withConductorDomain(null));
    assertThrows(IllegalArgumentException.class, () -> config.withExecutorId(""));
    assertDoesNotThrow(() -> config.withExecutorId(null));
    assertThrows(IllegalArgumentException.class, () -> config.withAppVersion(""));
    assertDoesNotThrow(() -> config.withAppVersion(null));
  }

  @Test
  public void calcAppVersion() throws Exception {
    var config = pgContainer.dbosConfig();
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImpl());
      dbos.launch();

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
      var version =
          assertDoesNotThrow(
              () ->
                  AppVersionComputer.computeAppVersion(
                      DBOS.version(), dbosExecutor.getRegisteredWorkflows()));
      assertFalse(version.startsWith("unknown"));
      assertEquals(version, dbosExecutor.appVersion());
    }
  }

  @Test
  public void calcAppVersionNotMatchAcrossDbosVersions() throws Exception {
    var config = pgContainer.dbosConfig();
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImpl());
      dbos.launch();

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
      var version =
          assertDoesNotThrow(
              () ->
                  AppVersionComputer.computeAppVersion(
                      "foo" + DBOS.version(), dbosExecutor.getRegisteredWorkflows()));
      assertFalse(version.startsWith("unknown"));
      assertNotEquals(version, dbosExecutor.appVersion());
    }
  }

  @Test
  public void calcAppVersionMatch() throws Exception {
    var config = pgContainer.dbosConfig();
    String appVer1;
    // calculate the baseline app version
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImpl());
      dbos.launch();
      var exec = DBOSTestAccess.getDbosExecutor(dbos);
      appVer1 = exec.appVersion();
    }

    // ensure app version calculation matches when using the same impl class
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImpl());
      dbos.launch();
      var exec = DBOSTestAccess.getDbosExecutor(dbos);
      assertEquals(appVer1, exec.appVersion());
    }

    // ensure app version calculation matches when using the different impl class but with the same
    // class name & workflow function
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImplAgain());
      dbos.launch();
      var exec = DBOSTestAccess.getDbosExecutor(dbos);
      assertEquals(appVer1, exec.appVersion());
    }
  }

  @Test
  public void calcAppVersionNotMatch() throws Exception {
    var config = pgContainer.dbosConfig();
    String appVer1;
    // calculate the baseline app version
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImpl());
      dbos.launch();
      var exec = DBOSTestAccess.getDbosExecutor(dbos);
      appVer1 = exec.appVersion();
    }

    // ensure app version for same class but with instance name is different
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImpl(), "instance-name");
      dbos.launch();
      var exec = DBOSTestAccess.getDbosExecutor(dbos);
      assertNotEquals(appVer1, exec.appVersion());
    }

    // ensure app version for class with same name but different @Workflow function is different
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImplDifferentCode());
      dbos.launch();
      var exec = DBOSTestAccess.getDbosExecutor(dbos);
      assertNotEquals(appVer1, exec.appVersion());
    }

    // ensure app version for class with same @Workflow function but different name is different
    try (var dbos = new DBOS(config)) {
      dbos.registerProxy(ConfigTestService.class, new ConfigTestServiceImplDifferentName());
      dbos.launch();
      var exec = DBOSTestAccess.getDbosExecutor(dbos);
      assertNotEquals(appVer1, exec.appVersion());
    }
  }

  @Test
  public void configPGSimpleDataSource() throws Exception {

    var jdbcUrl = pgContainer.jdbcUrl();
    assertTrue(jdbcUrl.startsWith("jdbc:"));

    var uri = URI.create(jdbcUrl.substring(5));
    assertTrue(uri.getPath().startsWith("/"));
    assertTrue(uri.getPort() != -1);

    var ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {uri.getHost()});
    ds.setDatabaseName(uri.getPath().substring(1));
    ds.setUser(pgContainer.username());
    ds.setPassword(pgContainer.password());
    ds.setPortNumbers(new int[] {uri.getPort()});

    var config =
        DBOSConfig.defaults("config-test")
            .withDataSource(ds)
            // Intentionally set an invalid URL and credentials to verify that when a DataSource
            // is provided, these values are ignored and do not affect connectivity.
            .withDatabaseUrl("completely-invalid-url")
            .withDbUser("invalid-user")
            .withDbPassword("invalid-password");
    var dbos = new DBOS(config);

    try {
      var proxy = dbos.registerProxy(ExecutorTestService.class, new ExecutorTestServiceImpl(dbos));
      dbos.launch();

      var options = new StartWorkflowOptions("dswfid");
      var handle = dbos.startWorkflow(() -> proxy.workflow(), options);
      assertEquals(6, handle.getResult());
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    } finally {
      dbos.shutdown();
    }
  }

  @Test
  public void configHikariDataSource() throws Exception {

    var poolName = "dbos-configDataSource";

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(pgContainer.jdbcUrl());
    hikariConfig.setUsername(pgContainer.username());
    hikariConfig.setPassword(pgContainer.password());
    hikariConfig.setPoolName(poolName);

    try (var dataSource = new HikariDataSource(hikariConfig)) {
      assertFalse(dataSource.isClosed());
      var config =
          DBOSConfig.defaults("config-test")
              .withDataSource(dataSource)
              // Intentionally set an invalid URL and credentials to verify that when a DataSource
              // is provided, these values are ignored and do not affect connectivity.
              .withDatabaseUrl("completely-invalid-url")
              .withDbUser("invalid-user")
              .withDbPassword("invalid-password");
      var dbos = new DBOS(config);

      try {
        var proxy =
            dbos.registerProxy(ExecutorTestService.class, new ExecutorTestServiceImpl(dbos));
        dbos.launch();

        var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
        var dbConfig = DBTestAccess.findHikariConfig(sysdb);
        assertTrue(dbConfig.isPresent());
        assertEquals(poolName, dbConfig.get().getPoolName());

        var options = new StartWorkflowOptions("dswfid");
        var handle = dbos.startWorkflow(() -> proxy.workflow(), options);
        assertEquals(6, handle.getResult());
        assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
      } finally {
        dbos.shutdown();
      }
    }
  }

  @Test
  public void dbosVersion() throws Exception {
    Assumptions.assumeFalse(
        DBOS.version().equals("${projectVersion}"), "skipping, DBOS version not set");

    assertNotNull(DBOS.version());
    assertFalse(DBOS.version().contains("unknown"));
    var version = assertDoesNotThrow(() -> new ComparableVersion(DBOS.version()));

    // an invalid version string will be parsed as 0.0-qualifier, so make sure
    // the value provided is later 0.6 (the initial published version)
    assertTrue(version.compareTo(new ComparableVersion("0.6")) > 0);
  }
}
