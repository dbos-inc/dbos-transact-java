package dev.dbos.transact.cli;

import dev.dbos.transact.DBOSClient;

import picocli.CommandLine.Option;

public class DatabaseOptions {
  @Option(
      names = {"-D", "--db-url"},
      defaultValue = "${DBOS_SYSTEM_JDBC_URL}",
      description = "Your DBOS system database URL [default: DBOS_SYSTEM_JDBC_URL env var]")
  private String url;

  @Option(
      names = {"-U", "--db-user"},
      defaultValue = "${PGUSER}",
      description = "user name for your DBOS system database [default: PGUSER env var]")
  private String user;

  @Option(
      names = {"-P", "--db-password"},
      defaultValue = "${PGPASSWORD}",
      description = "password for your DBOS system database [default: PGPASSWORD env var]",
      arity = "0..1",
      interactive = true)
  private String password;

  public String url() {
    return this.url;
  }

  public String user() {
    return this.user;
  }

  public String password() {
    return this.password;
  }

  public DBOSClient createClient() {
    return new DBOSClient(url(), user(), password());
  }
}
