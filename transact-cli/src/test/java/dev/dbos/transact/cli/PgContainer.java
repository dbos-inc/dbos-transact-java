package dev.dbos.transact.cli;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.testcontainers.postgresql.PostgreSQLContainer;

public class PgContainer implements AutoCloseable {
  private final PostgreSQLContainer pgContainer = new PostgreSQLContainer("postgres:18");

  public PgContainer() {
    pgContainer.start();
  }

  @Override
  public void close() throws Exception {
    pgContainer.stop();
  }

  public String jdbcUrl() {
    return pgContainer.getJdbcUrl();
  }

  public String username() {
    return pgContainer.getUsername();
  }

  public String password() {
    return pgContainer.getPassword();
  }

  public Connection connection() throws SQLException {
    return DriverManager.getConnection(jdbcUrl(), username(), password());
  }

  public List<String> options() {
    return List.of(urlOption(), userOption(), passwordOption());
  }

  public String urlOption() {
    return "-D=" + jdbcUrl();
  }

  public String userOption() {
    return "-U=" + username();
  }

  public String passwordOption() {
    return "-P=" + password();
  }
}
