package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.DbSetupTestBase;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.invocation.HawkService;
import dev.dbos.transact.invocation.HawkServiceImpl;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Workflow;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.ds.PGSimpleDataSource;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@ExtendWith(SystemStubsExtension.class)
public class ConfigTest extends DbSetupTestBase {

  @SystemStub private EnvironmentVariables envVars = new EnvironmentVariables();

  @Test
  public void configOverridesEnvAppVerAndExecutor() throws Exception {
    envVars.set("DBOS__VMID", "test-env-executor-id");
    envVars.set("DBOS__APPVERSION", "test-env-app-version");
    envVars.set("DBOS__APPID", "test-env-app-id");

    var config =
        createConfig("config-test")
            .withAppVersion("test-app-version")
            .withExecutorId("test-executor-id");

    DBOSTestAccess.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertEquals("test-app-version", dbosExecutor.appVersion());
      assertEquals("test-executor-id", dbosExecutor.executorId());
      assertEquals("test-env-app-id", dbosExecutor.appId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void envAppVerAndExecutor() throws Exception {

    envVars.set("DBOS__VMID", "test-env-executor-id");
    envVars.set("DBOS__APPVERSION", "test-env-app-version");
    envVars.set("DBOS__APPID", "test-env-app-id");

    var config = createConfig("config-test");

    DBOSTestAccess.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertEquals("test-env-app-version", dbosExecutor.appVersion());
      assertEquals("test-env-executor-id", dbosExecutor.executorId());
      assertEquals("test-env-app-id", dbosExecutor.appId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void dbosCloudEnvOverridesConfigAppVerAndExecutor() throws Exception {

    envVars.set("DBOS__CLOUD", "true");
    envVars.set("DBOS__VMID", "test-env-executor-id");
    envVars.set("DBOS__APPVERSION", "test-env-app-version");
    envVars.set("DBOS__APPID", "test-env-app-id");

    var config =
        createConfig("config-test")
            .withAppVersion("test-app-version")
            .withExecutorId("test-executor-id");

    DBOSTestAccess.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertEquals("test-env-app-version", dbosExecutor.appVersion());
      assertEquals("test-env-executor-id", dbosExecutor.executorId());
      assertEquals("test-env-app-id", dbosExecutor.appId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void localExecutorId() throws Exception {
    var config = createConfig("config-test");

    DBOSTestAccess.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertEquals("local", dbosExecutor.executorId());
      assertEquals("", dbosExecutor.appId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void conductorExecutorId() throws Exception {
    var config = createConfigFromEnv("config-test").withConductorKey("test-conductor-key");

    DBOSTestAccess.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertNotNull(dbosExecutor.executorId());
      assertDoesNotThrow(() -> UUID.fromString(dbosExecutor.executorId()));
      assertEquals("", dbosExecutor.appId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void cantSetExecutorIdWhenUsingConductor() throws Exception {
    var config =
        createConfigFromEnv("config-test")
            .withConductorKey("test-conductor-key")
            .withExecutorId("test-executor-id");

    DBOSTestAccess.reinitialize(config);
    try {
      assertThrows(IllegalArgumentException.class, () -> DBOS.launch());
    } finally {
      DBOS.shutdown();
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
    var config = createConfig("config-test");

    DBOSTestAccess.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      List<Class<?>> workflowClasses =
          dbosExecutor.getWorkflows().stream()
              .map(r -> r.target().getClass())
              .collect(Collectors.toList());
      var version = assertDoesNotThrow(() -> AppVersionComputer.computeAppVersion(workflowClasses));
      assertEquals(version, dbosExecutor.appVersion());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void configPGSimpleDataSource() throws Exception {
    var url = dbosConfig.databaseUrl();
    var user = dbosConfig.dbUser();
    var password = dbosConfig.dbPassword();
    DBUtils.recreateDB(url, user, password);

    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {postgres.getHost()});
    ds.setDatabaseName(postgres.getDatabaseName());
    ds.setUser(user);
    ds.setPassword(password);
    ds.setPortNumbers(new int[] {postgres.getFirstMappedPort()});

    var config =
        DBOSConfig.defaults("config-test")
            .withDataSource(ds)
            // Intentionally set an invalid URL and credentials to verify that when a DataSource
            // is provided, these values are ignored and do not affect connectivity.
            .withDatabaseUrl("completely-invalid-url")
            .withDbUser("invalid-user")
            .withDbPassword("invalid-password");

    DBOSTestAccess.reinitialize(config);

    var impl = new HawkServiceImpl();
    var proxy = DBOS.registerWorkflows(HawkService.class, impl);
    impl.setProxy(proxy);

    try {
      DBOS.launch();

      var options = new StartWorkflowOptions("dswfid");
      var handle = DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options);
      var result = handle.getResult();
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
      assertEquals("SUCCESS", handle.getStatus().status());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void configHikariDataSource() throws Exception {

    var poolName = "dbos-configDataSource";
    var url = dbosConfig.databaseUrl();
    var user = dbosConfig.dbUser();
    var password = dbosConfig.dbPassword();

    DBUtils.recreateDB(url, user, password);

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(url);
    hikariConfig.setUsername(user);
    hikariConfig.setPassword(password);
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

      DBOSTestAccess.reinitialize(config);
      assertFalse(dataSource.isClosed());

      var impl = new HawkServiceImpl();
      var proxy = DBOS.registerWorkflows(HawkService.class, impl);
      impl.setProxy(proxy);

      try {
        DBOS.launch();

        var sysdb = DBOSTestAccess.getSystemDatabase();
        var dbConfig = DBTestAccess.getHikariConfig(sysdb);
        assertTrue(dbConfig.isPresent());
        assertEquals(poolName, dbConfig.get().getPoolName());

        var options = new StartWorkflowOptions("dswfid");
        var handle = DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options);
        var result = handle.getResult();
        assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
        assertEquals("SUCCESS", handle.getStatus().status());
      } finally {
        DBOS.shutdown();
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
  public void appVersion() throws Exception {
    try {
      envVars.set("DBOS__APPID", "test-env-app-id");
      var dbosConfig = createConfigFromEnv("systemdbtest");
      DBUtils.recreateDB(dbosConfig);
      DBOSTestAccess.reinitialize(dbosConfig);

      var proxy = DBOS.registerWorkflows(ExecutorTestService.class, new ExecutorTestServiceImpl());
      DBOS.launch();

      var handle = DBOS.startWorkflow(() -> proxy.workflow());
      assertEquals(6, handle.getResult());

      var input = new ListWorkflowsInput().withWorkflowId(handle.workflowId());
      var workflows = DBOS.listWorkflows(input);
      assertEquals(1, workflows.size());
      assertEquals("test-env-app-id", workflows.get(0).appId());
    } finally {
      DBOS.shutdown();
    }
  }
}

interface ExecutorTestService {
  Integer workflow();
}

class ExecutorTestServiceImpl implements ExecutorTestService {

  @Override
  @Workflow
  public Integer workflow() {
    var a = DBOS.runStep(() -> 1, "stepOne");
    var b = DBOS.runStep(() -> 2, "stepTwo");
    var c = DBOS.runStep(() -> 3, "stepThree");
    return a + b + c;
  }
}
