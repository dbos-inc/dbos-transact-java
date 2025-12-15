package dev.dbos.transact.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.invocation.HawkService;
import dev.dbos.transact.invocation.HawkServiceImpl;
import dev.dbos.transact.utils.DBUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
public class ConfigTest {

  @SystemStub private EnvironmentVariables envVars = new EnvironmentVariables();

  @Test
  public void configOverridesEnvAppVerAndExecutor() throws Exception {
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
      assertEquals("test-app-version", dbosExecutor.appVersion());
      assertEquals("test-executor-id", dbosExecutor.executorId());
    } finally {
      DBOS.shutdown();
    }
  }

  @Test
  public void envAppVerAndExecutor() throws Exception {

    envVars.set("DBOS__VMID", "test-env-executor-id");
    envVars.set("DBOS__APPVERSION", "test-env-app-version");

    var config =
        DBOSConfig.defaults("config-test")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres")
            .withDbPassword(System.getenv("PGPASSWORD"));

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
  public void dbosCloudEnvOverridesConfigAppVerAndExecutor() throws Exception {

    envVars.set("DBOS__CLOUD", "true");
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
  public void cantSetExecutorIdWhenUsingConductor() throws Exception {
    var config =
        DBOSConfig.defaultsFromEnv("config-test")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withConductorKey("test-conductor-key")
            .withExecutorId("test-executor-id");

    DBOS.reinitialize(config);
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
    var config =
        DBOSConfig.defaults("config-test")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres")
            .withDbPassword(System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR));

    DBOS.reinitialize(config);
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
}
