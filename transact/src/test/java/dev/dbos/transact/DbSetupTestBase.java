package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.postgresql.PostgreSQLContainer;

public class DbSetupTestBase {

  protected static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

  protected static DBOSConfig dbosConfig;
  protected static HikariDataSource dataSource;

  @BeforeAll
  protected static void onetimeSetup() throws Exception {
    postgres.start();
    dbosConfig =
        DBOSConfig.defaults("systemdbtest")
            .withDatabaseUrl(postgres.getJdbcUrl())
            .withDbUser(postgres.getUsername())
            .withDbPassword(postgres.getPassword());
    dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterAll
  protected static void afterAll() throws Exception {
    postgres.stop();
  }

  protected DBOSClient getDBOSClient() {
    return new DBOSClient(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  protected static DBOSConfig createConfig(String appName) {
    return DBOSConfig.defaults(appName)
        .withDatabaseUrl(postgres.getJdbcUrl())
        .withDbUser(postgres.getUsername())
        .withDbPassword(postgres.getPassword());
  }

  protected static DBOSConfig createConfigFromEnv(String appName) {
    return DBOSConfig.defaultsFromEnv(appName)
        .withDatabaseUrl(postgres.getJdbcUrl())
        .withDbUser(postgres.getUsername())
        .withDbPassword(postgres.getPassword());
  }

  protected static String getJdbcUrl(String databaseName) {
    return postgres.getJdbcUrl().replaceFirst("/[^/?]+(\\?.*)?$", "/" + databaseName + "$1");
  }
}
