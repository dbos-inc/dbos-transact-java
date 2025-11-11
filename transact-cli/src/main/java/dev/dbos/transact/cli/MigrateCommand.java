package dev.dbos.transact.cli;

import dev.dbos.transact.migrations.MigrationManager;

import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(
    name = "migrate",
    description = "Create DBOS system tables",
    mixinStandardHelpOptions = true)
public class MigrateCommand implements Callable<Integer> {

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-r", "--app-role"},
      description = "The role with which you will run your DBOS application")
  String appRole;

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    var out = spec.commandLine().getOut();
    out.format("Starting DBOS migrations");
    out.format("  System Database: %s", dbOptions.url);
    out.format("  System Database User: %s", dbOptions.user);

    MigrationManager.runMigrations(dbOptions.url, dbOptions.user, dbOptions.password);
    grantDBOSSchemaPermissions(out);
    return 0;
  }

  void grantDBOSSchemaPermissions(PrintWriter out) throws SQLException {
    final var schema = "dbos";

    if (appRole == null || appRole.isEmpty()) {
      return;
    }

    out.format(
        "Granting permissions for the %s schema to %s in database %s",
        schema, appRole, dbOptions.url);

    String[] queries = {
      "GRANT USAGE ON SCHEMA %s TO %s",
      "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO %s",
      "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s TO %s",
      "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %s TO %s",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON TABLES TO %s",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON SEQUENCES TO %s",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO %s"
    };
    try (var conn = DriverManager.getConnection(dbOptions.url, dbOptions.user, dbOptions.password);
        var stmt = conn.createStatement()) {
      for (var query : queries) {
        query = query.formatted(schema, appRole);
        stmt.execute(query);
      }
    }
  }
}
