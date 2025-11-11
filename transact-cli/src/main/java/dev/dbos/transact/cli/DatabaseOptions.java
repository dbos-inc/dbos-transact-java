package dev.dbos.transact.cli;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOSClient;

import java.util.Objects;

import picocli.CommandLine.Option;

public class DatabaseOptions {
  @Option(
      names = {"-D", "--db-url"},
      description = "Your DBOS system database URL (defaults to DBOS_SYSTEM_JDBC_URL env var)")
  private String url;

  @Option(
      names = {"-U", "--db-user"},
      description = "user name for your DBOS system database (defaults to PGUSER env var)")
  private String user;

  @Option(
      names = {"-P", "--db-password"},
      description = "password for your DBOS system database (defaults to PGPASSWORD env var)",
      arity = "0..1",
      interactive = true)
  private String password;

  public String url() {
    return Objects.requireNonNullElseGet(
        this.url, () -> System.getenv(Constants.SYSTEM_JDBC_URL_ENV_VAR));
  }

  public String user() {
    return Objects.requireNonNullElseGet(
        this.user, () -> System.getenv(Constants.POSTGRES_USER_ENV_VAR));
  }

  public String password() {
    return Objects.requireNonNullElseGet(
        this.password, () -> System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR));
  }

  public DBOSClient createClient() {
    return new DBOSClient(url(), user(), password());
  }
}
