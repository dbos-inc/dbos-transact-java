package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.invocation.HawkService;
import dev.dbos.transact.invocation.HawkServiceImpl;
import dev.dbos.transact.utils.DBUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.maven.artifact.versioning.ComparableVersion;
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
      var expected = "6482a0dde9a452189b20c5f5e0d00a661ea8f160d58244cfc0a99cc5f13dbcad";
      assertEquals(expected, dbosExecutor.appVersion());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void configDataSource() throws Exception {

    var poolName = "dbos-configDataSource";
    var url = "jdbc:postgresql://localhost:5432/dbos_java_sys";
    var user = "postgres";
    var password = System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR);

    DBUtils.recreateDB(url, user, password);

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(url);
    hikariConfig.setUsername(user);
    hikariConfig.setPassword(password);
    hikariConfig.setPoolName(poolName);

    var dataSource = new HikariDataSource(hikariConfig);
    assertFalse(dataSource.isClosed());
    var config =
        DBOSConfig.defaults("config-test")
            .withDataSource(dataSource)
            .withDatabaseUrl("completely-invalid-url")
            .withDbUser("invalid-user")
            .withDbPassword("invalid-password");

    DBOS.reinitialize(config);
    assertFalse(dataSource.isClosed());

    var impl = new HawkServiceImpl();
    var proxy = DBOS.registerWorkflows(HawkService.class, impl);
    impl.setProxy(proxy);

    try {
      DBOS.launch();

      var sysdb = DBOSTestAccess.getSystemDatabase();
      var dbConfig = DBTestAccess.getHikariConfig(sysdb);
      assertEquals(poolName, dbConfig.getPoolName());

      var result = proxy.simpleWorkflow();
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);

    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void dbosVersion() throws Exception {
    assertNotNull(DBOS.version());
    assertFalse(DBOS.version().contains("unknown"));
    var version = assertDoesNotThrow(() -> new ComparableVersion(DBOS.version()));

    // an invalid version string will be parsed as 0.0-qualifier, so make sure 
    // the value provided is later 0.6 (the initial published version)
    assertTrue(version.compareTo(new ComparableVersion("0.6")) > 0);
  }
}
