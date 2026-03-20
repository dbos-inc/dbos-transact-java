package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Workflow;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.postgresql.ds.PGSimpleDataSource;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

interface ExecutorTestService {
  Integer workflow();
}

class ExecutorTestServiceImpl implements ExecutorTestService {

  private final DBOS dbos;

  public ExecutorTestServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public Integer workflow() {
    var a = dbos.runStep(() -> 1, "stepOne");
    var b = dbos.runStep(() -> 2, "stepTwo");
    var c = dbos.runStep(() -> 3, "stepThree");
    return a + b + c;
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class ConfigTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @Test
  @Execution(ExecutionMode.SAME_THREAD)
  public void configOverridesEnvAppVerAndExecutor() throws Exception {
    var envVars =
        new EnvironmentVariables("DBOS__VMID", "test-env-executor-id")
            .and("DBOS__APPVERSION", "test-env-app-version")
            .and("DBOS__APPID", "test-env-app-id");

    envVars.execute(
        () -> {
          var config =
              pgContainer
                  .dbosConfig()
                  .withAppVersion("test-app-version")
                  .withExecutorId("test-executor-id");
          var dbos = new DBOS(config);

          try {
            dbos.launch();
            var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
            assertEquals("test-app-version", dbosExecutor.appVersion());
            assertEquals("test-executor-id", dbosExecutor.executorId());
            assertEquals("test-env-app-id", dbosExecutor.appId());
          } finally {
            dbos.shutdown();
          }
        });
  }

  @Test
  @Execution(ExecutionMode.SAME_THREAD)
  public void envAppVerAndExecutor() throws Exception {
    var envVars =
        new EnvironmentVariables("DBOS__VMID", "test-env-executor-id")
            .and("DBOS__APPVERSION", "test-env-app-version")
            .and("DBOS__APPID", "test-env-app-id");

    envVars.execute(
        () -> {
          var config = pgContainer.dbosConfig();
          var dbos = new DBOS(config);

          try {
            dbos.launch();
            var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
            assertEquals("test-env-app-version", dbosExecutor.appVersion());
            assertEquals("test-env-executor-id", dbosExecutor.executorId());
            assertEquals("test-env-app-id", dbosExecutor.appId());
          } finally {
            dbos.shutdown();
          }
        });
  }

  @Test
  @Execution(ExecutionMode.SAME_THREAD)
  public void dbosCloudEnvOverridesConfigAppVerAndExecutor() throws Exception {
    var envVars =
        new EnvironmentVariables("DBOS__CLOUD", "true")
            .and("DBOS__VMID", "test-env-executor-id")
            .and("DBOS__APPVERSION", "test-env-app-version")
            .and("DBOS__APPID", "test-env-app-id");

    envVars.execute(
        () -> {
          var config =
              pgContainer
                  .dbosConfig()
                  .withAppVersion("test-app-version")
                  .withExecutorId("test-executor-id");
          var dbos = new DBOS(config);

          try {
            dbos.launch();
            var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
            assertEquals("test-env-app-version", dbosExecutor.appVersion());
            assertEquals("test-env-executor-id", dbosExecutor.executorId());
            assertEquals("test-env-app-id", dbosExecutor.appId());
          } finally {
            dbos.shutdown();
          }
        });
  }

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
    var dbos = new DBOS(config);
    try {
      dbos.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
      List<Class<?>> workflowClasses =
          dbosExecutor.getWorkflows().stream()
              .map(r -> r.target().getClass())
              .collect(Collectors.toList());
      var version = assertDoesNotThrow(() -> AppVersionComputer.computeAppVersion(workflowClasses));
      assertEquals(version, dbosExecutor.appVersion());
    } finally {
      dbos.shutdown();
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
      var proxy =
          dbos.registerWorkflows(ExecutorTestService.class, new ExecutorTestServiceImpl(dbos));
      dbos.launch();

      var options = new StartWorkflowOptions("dswfid");
      var handle = dbos.startWorkflow(() -> proxy.workflow(), options);
      assertEquals(6, handle.getResult());
      assertEquals("SUCCESS", handle.getStatus().status());
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
            dbos.registerWorkflows(ExecutorTestService.class, new ExecutorTestServiceImpl(dbos));
        dbos.launch();

        var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
        var dbConfig = DBTestAccess.getHikariConfig(sysdb);
        assertTrue(dbConfig.isPresent());
        assertEquals(poolName, dbConfig.get().getPoolName());

        var options = new StartWorkflowOptions("dswfid");
        var handle = dbos.startWorkflow(() -> proxy.workflow(), options);
        assertEquals(6, handle.getResult());
        assertEquals("SUCCESS", handle.getStatus().status());
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

  @Test
  @Execution(ExecutionMode.SAME_THREAD)
  public void appVersion() throws Exception {
    var envVars = new EnvironmentVariables("DBOS__APPID", "test-env-app-id");

    envVars.execute(
        () -> {
          var dbosConfig = pgContainer.dbosConfig();
          var dbos = new DBOS(dbosConfig);

          try {
            var proxy =
                dbos.registerWorkflows(
                    ExecutorTestService.class, new ExecutorTestServiceImpl(dbos));
            dbos.launch();

            var handle = dbos.startWorkflow(() -> proxy.workflow());
            assertEquals(6, handle.getResult());

            var input = new ListWorkflowsInput().withWorkflowId(handle.workflowId());
            var workflows = dbos.listWorkflows(input);
            assertEquals(1, workflows.size());
            assertEquals("test-env-app-id", workflows.get(0).appId());
          } finally {
            dbos.shutdown();
          }
        });
  }
}
