package dev.dbos.transact.utils;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

public class PgContainer {
  private final PostgreSQLContainer pgContainer = new PostgreSQLContainer("postgres:18");

  public String jdbcUrl() {
    return pgContainer.getJdbcUrl();
  }

  public String username() {
    return pgContainer.getUsername();
  }

  public String password() {
    return pgContainer.getPassword();
  }

  public void start() {
    pgContainer.start();
  }

  public void stop() {
    pgContainer.stop();
  }

  public DBOSConfig dbosConfig() {
    return dbosConfig(null);
  }

  public DBOSConfig dbosConfig(String appName) {
    return DBOSConfig.defaults(Objects.requireNonNullElse(appName, "transact-java-test"))
        .withDatabaseUrl(jdbcUrl())
        .withDbUser(username())
        .withDbPassword(password());
  }

  public HikariDataSource dataSource() {
    return SystemDatabase.createDataSource(jdbcUrl(), username(), password());
  }

  public DBOSClient dbosClient() {
    return new DBOSClient(jdbcUrl(), username(), password());
  }
}
