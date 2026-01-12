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
  static void onetimeSetup() {
    postgres.start();
    dbosConfig =
        DBOSConfig.defaults("systemdbtest")
            .withDatabaseUrl(postgres.getJdbcUrl())
            .withDbUser(postgres.getUsername())
            .withDbPassword(postgres.getPassword())
            .withMaximumPoolSize(2);
    dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterAll
  static void afterAll() {
    postgres.stop();
  }

  protected DBOSClient getDBOSClient() {
    return new DBOSClient(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }
}
