package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.VersionInfo;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

// Environment Variables are JVM global state. This test class has all the tests that require
// setting env vars and runs them @Isolated and on the SAME_THREAD (thus, serializing their
// execuition)
@Isolated
@Execution(ExecutionMode.SAME_THREAD)
public class ConfigEnvTest {

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
  public void testAppVersion() throws Exception {
    String versionOne;
    {
      var d = new DBOS(pgContainer.dbosConfig());
      d.registerProxy(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
      d.launch();
      versionOne = DBOSTestAccess.getDbosExecutor(d).appVersion();
      assertTrue(versionOne.length() > 0);
      assertTrue(versionOne.matches("[0-9a-fA-F]+"));
      d.shutdown();
    }

    {
      var d = new DBOS(pgContainer.dbosConfig());
      d.registerProxy(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
      d.launch();
      assertEquals(versionOne, DBOSTestAccess.getDbosExecutor(d).appVersion());
      d.shutdown();
    }

    String versionTwo;
    {
      var d = new DBOS(pgContainer.dbosConfig());
      d.registerProxy(AltVersionService.class, new AltVersionServiceImpl(d));
      d.launch();
      versionTwo = DBOSTestAccess.getDbosExecutor(d).appVersion();
      assertNotEquals(versionOne, versionTwo);
      d.shutdown();
    }

    var versionThree = UUID.randomUUID().toString();
    new EnvironmentVariables("DBOS__APPVERSION", versionThree)
        .execute(
            () -> {
              var d = new DBOS(pgContainer.dbosConfig());
              d.registerProxy(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
              d.launch();
              assertEquals(versionThree, DBOSTestAccess.getDbosExecutor(d).appVersion());
              d.shutdown();
            });

    var versionFour = UUID.randomUUID().toString();
    var testExecutorId = UUID.randomUUID().toString();
    {
      var d =
          new DBOS(
              pgContainer.dbosConfig().withAppVersion(versionFour).withExecutorId(testExecutorId));
      var proxy = d.registerProxy(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
      d.launch();
      assertEquals(versionFour, DBOSTestAccess.getDbosExecutor(d).appVersion());
      assertEquals(testExecutorId, DBOSTestAccess.getDbosExecutor(d).executorId());
      var handle = d.startWorkflow(() -> proxy.workflow());
      handle.getResult();
      assertEquals(versionFour, handle.getStatus().appVersion());
      assertEquals(testExecutorId, handle.getStatus().executorId());
      d.shutdown();
    }

    var versionFive = UUID.randomUUID().toString();
    var d = new DBOS(pgContainer.dbosConfig().withAppVersion(versionFive));
    d.registerProxy(ExecutorTestService.class, new ExecutorTestServiceImpl(d));
    d.launch();

    try {
      var createdVersions = Set.of(versionOne, versionTwo, versionThree, versionFour, versionFive);

      var versions = d.listApplicationVersions();
      var versionNames =
          versions.stream().map(VersionInfo::versionName).collect(Collectors.toSet());
      assertEquals(createdVersions, versionNames);

      for (var v : versions) {
        assertNotNull(v.createdAt());
        assertTrue(v.createdAt().toEpochMilli() > 0);
      }

      var latest = d.getLatestApplicationVersion();
      assertEquals(versionFive, latest.versionName());
      assertNotNull(latest.createdAt());

      var versionFourCreatedAt =
          versions.stream()
              .filter(v -> v.versionName().equals(versionFour))
              .findFirst()
              .orElseThrow()
              .createdAt();

      Thread.sleep(100);
      d.setLatestApplicationVersion(versionFour);
      latest = d.getLatestApplicationVersion();
      assertEquals(versionFour, latest.versionName());
      assertEquals(versionFourCreatedAt, latest.createdAt());
      assertEquals(versionFour, d.listApplicationVersions().get(0).versionName());

      try (var client = pgContainer.dbosClient()) {
        var clientVersionNames =
            client.listApplicationVersions().stream()
                .map(VersionInfo::versionName)
                .collect(Collectors.toSet());
        assertEquals(createdVersions, clientVersionNames);

        for (var v : client.listApplicationVersions()) {
          assertNotNull(v.createdAt());
          assertTrue(v.createdAt().toEpochMilli() > 0);
        }

        var clientLatest = client.getLatestApplicationVersion();
        assertEquals(versionFour, clientLatest.versionName());
        assertNotNull(clientLatest.createdAt());

        var versionFiveCreatedAt =
            client.listApplicationVersions().stream()
                .filter(v -> v.versionName().equals(versionFive))
                .findFirst()
                .orElseThrow()
                .createdAt();

        Thread.sleep(100);
        client.setLatestApplicationVersion(versionFive);
        clientLatest = client.getLatestApplicationVersion();
        assertEquals(versionFive, clientLatest.versionName());
        assertEquals(versionFiveCreatedAt, clientLatest.createdAt());

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
                dbos.registerProxy(ExecutorTestService.class, new ExecutorTestServiceImpl(dbos));
            dbos.launch();

            var handle = dbos.startWorkflow(() -> proxy.workflow());
            assertEquals(6, handle.getResult());

            var input = new ListWorkflowsInput(handle.workflowId());
            var workflows = dbos.listWorkflows(input);
            assertEquals(1, workflows.size());
            assertEquals("test-env-app-id", workflows.get(0).appId());
          } finally {
            dbos.shutdown();
          }
        });
  }
}
