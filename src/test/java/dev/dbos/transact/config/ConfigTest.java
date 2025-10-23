package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
public class ConfigTest {

  @SystemStub private EnvironmentVariables envVars = new EnvironmentVariables();

  @Test
  public void setExecutorAndAppVersionViaConfig() throws Exception {
    var config =
        DBOSConfig.defaults("config-test")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres")
            .withDbPassword(System.getenv("PGPASSWORD"))
            .withAppVersion("test-app-version")
            .withExecutorId("test-executor-id");

    DBOS.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertEquals("test-app-version", dbosExecutor.appVersion());
      assertEquals("test-executor-id", dbosExecutor.executorId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void setExecutorAndAppVersionViaEnv() throws Exception {

    envVars.set("DBOS__VMID", "test-env-executor-id");
    envVars.set("DBOS__APPVERSION", "test-env-app-version");

    var config =
        DBOSConfig.defaults("config-test")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres")
            .withDbPassword(System.getenv("PGPASSWORD"))
            .withAppVersion("test-app-version")
            .withExecutorId("test-executor-id");

    DBOS.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertEquals("test-env-app-version", dbosExecutor.appVersion());
      assertEquals("test-env-executor-id", dbosExecutor.executorId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void localExecutorId() throws Exception {
    var config =
        DBOSConfig.defaults("config-test")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres")
            .withDbPassword(System.getenv("PGPASSWORD"));

    DBOS.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertEquals("local", dbosExecutor.executorId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void conductorExecutorId() throws Exception {
    var config =
        DBOSConfig.defaultsFromEnv("config-test")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withConductorKey("test-conductor-key");

    DBOS.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      assertNotNull(dbosExecutor.executorId());
      assertDoesNotThrow(() -> UUID.fromString(dbosExecutor.executorId()));
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void calcAppVersion() throws Exception {
    var config =
        DBOSConfig.defaults("config-test")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres")
            .withDbPassword(System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR));

    DBOS.reinitialize(config);
    try {
      DBOS.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor();
      // If we change the internally registered workflows, the expected value will change
      var expected = "16776e05fc693102206e4b6cb96c05f53e37ba5b6ea62b86d8daa3658263d407";
      assertEquals(expected, dbosExecutor.appVersion());
    } finally {
      DBOS.shutdown();
    }
  }
}
