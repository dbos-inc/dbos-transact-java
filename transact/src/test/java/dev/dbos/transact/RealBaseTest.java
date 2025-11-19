package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

public class RealBaseTest {

  protected static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");

  protected static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() {
    postgres.start();
    dbosConfig =
        DBOSConfig.defaults("systemdbtest")
            .withDatabaseUrl(postgres.getJdbcUrl())
            .withDbUser(postgres.getUsername())
            .withDbPassword(postgres.getPassword())
            .withMaximumPoolSize(2);
  }

  @AfterAll
  static void afterAll() {
    postgres.stop();
  }

  protected DBOSClient getDBOSClient() {
    return new DBOSClient(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }
}
