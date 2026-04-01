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
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.Workflow;

import java.net.URI;
import java.util.List;
import java.util.Set;
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
import org.junit.jupiter.api.parallel.Isolated;
import org.postgresql.ds.PGSimpleDataSource;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

interface ExecutorTestService {
  Integer workflow();
}

// A distinct workflow class used to verify that different registered classes produce a different
// computed app version than ExecutorTestService / ExecutorTestServiceImpl.
interface AltVersionService {
  String noop();
}

class AltVersionServiceImpl implements AltVersionService {

  private final DBOS dbos;

  public AltVersionServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String noop() {
    return dbos.runStep(() -> "noop", "noopStep");
  }
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

// @Isolated + @Execution(SAME_THREAD) together fully serialize this class:
//   - SAME_THREAD: all methods within ConfigTest run sequentially on one thread
//   - @Isolated: no other test class runs concurrently while ConfigTest executes
// This is required because several tests temporarily mutate JVM-global environment variables
// (DBOS__APPVERSION, DBOS__VMID, etc.) via EnvironmentVariables.execute(). That mutation is
// visible to all threads — not just the thread running the lambda — so without isolation,
// concurrent tests can pick up spurious values and fail.
@Isolated
@Execution(ExecutionMode.SAME_THREAD)
@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class ConfigTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @Test
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
  public void testAppVersion() throws Exception {
    // versionOne: auto-computed from registered workflow classes; verify it is a hex string
    String versionOne;
    {
      var d = new DBOS(pgContainer.dbosConfig());
      d.registerWorkflows(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
      d.launch();
      versionOne = DBOSTestAccess.getDbosExecutor(d).appVersion();
      assertTrue(versionOne.length() > 0);
      assertTrue(versionOne.matches("[0-9a-fA-F]+"));
      d.shutdown();
    }

    // Stability: same workflow classes produce the same version on re-launch
    {
      var d = new DBOS(pgContainer.dbosConfig());
      d.registerWorkflows(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
      d.launch();
      assertEquals(versionOne, DBOSTestAccess.getDbosExecutor(d).appVersion());
      d.shutdown();
    }

    // versionTwo: a different registered class produces a different computed version
    String versionTwo;
    {
      var d = new DBOS(pgContainer.dbosConfig());
      d.registerWorkflows(AltVersionService.class, new AltVersionServiceImpl(d));
      d.launch();
      versionTwo = DBOSTestAccess.getDbosExecutor(d).appVersion();
      assertNotEquals(versionOne, versionTwo);
      d.shutdown();
    }

    // versionThree: override via environment variable
    var versionThree = UUID.randomUUID().toString();
    new EnvironmentVariables("DBOS__APPVERSION", versionThree)
        .execute(
            () -> {
              var d = new DBOS(pgContainer.dbosConfig());
              d.registerWorkflows(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
              d.launch();
              assertEquals(versionThree, DBOSTestAccess.getDbosExecutor(d).appVersion());
              d.shutdown();
            });

    // versionFour: override via config parameter; also verify executor ID and workflow status
    var versionFour = UUID.randomUUID().toString();
    var testExecutorId = UUID.randomUUID().toString();
    {
      var d =
          new DBOS(
              pgContainer.dbosConfig().withAppVersion(versionFour).withExecutorId(testExecutorId));
      var proxy = d.registerWorkflows(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
      d.launch();
      assertEquals(versionFour, DBOSTestAccess.getDbosExecutor(d).appVersion());
      assertEquals(testExecutorId, DBOSTestAccess.getDbosExecutor(d).executorId());
      var handle = d.startWorkflow(() -> proxy.workflow());
      handle.getResult();
      assertEquals(versionFour, handle.getStatus().appVersion());
      assertEquals(testExecutorId, handle.getStatus().executorId());
      d.shutdown();
    }

    // versionFive: another config override — this will be the latest version when CRUD tests begin
    var versionFive = UUID.randomUUID().toString();
    var d = new DBOS(pgContainer.dbosConfig().withAppVersion(versionFive));
    d.registerWorkflows(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
    d.launch();

    try {
      var createdVersions = Set.of(versionOne, versionTwo, versionThree, versionFour, versionFive);

      // ── Test version CRUD via DBOS API ────────────────────────────────────

      // listApplicationVersions returns exactly the versions created during this test
      var versions = d.listApplicationVersions();
      var versionNames =
          versions.stream().map(VersionInfo::versionName).collect(Collectors.toSet());
      assertEquals(createdVersions, versionNames);

      // createdAt is set and positive on all versions
      for (var v : versions) {
        assertNotNull(v.createdAt());
        assertTrue(v.createdAt().toEpochMilli() > 0);
      }

      // getLatestApplicationVersion returns the most recently launched version
      var latest = d.getLatestApplicationVersion();
      assertEquals(versionFive, latest.versionName());
      assertNotNull(latest.createdAt());

      // Record versionFour's createdAt before promoting it
      var versionFourCreatedAt =
          versions.stream()
              .filter(v -> v.versionName().equals(versionFour))
              .findFirst()
              .orElseThrow()
              .createdAt();

      // setLatestApplicationVersion changes which version is latest
      Thread.sleep(100);
      d.setLatestApplicationVersion(versionFour);
      latest = d.getLatestApplicationVersion();
      assertEquals(versionFour, latest.versionName());
      // createdAt must not change when promoting a version
      assertEquals(versionFourCreatedAt, latest.createdAt());
      // versionFour should now sort first (highest timestamp)
      assertEquals(versionFour, d.listApplicationVersions().get(0).versionName());

      // ── Test version CRUD via Client API ──────────────────────────────────

      try (var client = pgContainer.dbosClient()) {
        // Client sees exactly the same set of versions
        var clientVersionNames =
            client.listApplicationVersions().stream()
                .map(VersionInfo::versionName)
                .collect(Collectors.toSet());
        assertEquals(createdVersions, clientVersionNames);

        // createdAt is present and positive in client results
        for (var v : client.listApplicationVersions()) {
          assertNotNull(v.createdAt());
          assertTrue(v.createdAt().toEpochMilli() > 0);
        }

        // Client sees versionFour as latest (set via DBOS API above)
        var clientLatest = client.getLatestApplicationVersion();
        assertEquals(versionFour, clientLatest.versionName());
        assertNotNull(clientLatest.createdAt());

        // Record versionFive's createdAt before promoting it via the client
        var versionFiveCreatedAt =
            client.listApplicationVersions().stream()
                .filter(v -> v.versionName().equals(versionFive))
                .findFirst()
                .orElseThrow()
                .createdAt();

        // Set versionFive as latest via client
        Thread.sleep(100);
        client.setLatestApplicationVersion(versionFive);
        clientLatest = client.getLatestApplicationVersion();
        assertEquals(versionFive, clientLatest.versionName());
        // createdAt must not change when promoting via client
        assertEquals(versionFiveCreatedAt, clientLatest.createdAt());

        // DBOS API should see the same change made via the client
        assertEquals(versionFive, d.getLatestApplicationVersion().versionName());
      }
    } finally {
      d.shutdown();
    }
  }

  @Test
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
