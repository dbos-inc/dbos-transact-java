package dev.dbos.transact.cli;

import dev.dbos.transact.database.SystemDatabase;
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

@Command(name = "migrate", description = "Create DBOS system tables")
public class MigrateCommand implements Callable<Integer> {

  @Option(
      names = {"-r", "--app-role"},
      description = "The role with which you will run your DBOS application")
  String appRole;

  @Option(
      names = {"--listen-notify"},
      negatable = true,
      defaultValue = "true",
      description =
          "Use LISTEN/NOTIFY on the DBOS system database [default: ${DEFAULT-VALUE}]. Use --no-listen-notify to disable.")
  boolean useListenNotify;

  @Option(
      names = {"--print-migrations"},
      paramLabel = "[all|NUMBER]",
      description =
          "Print the SQL of all migrations ('--print-migrations all') or of migrations from a number onward ('--print-migrations 3') instead of running them")
  String printMigrations;

  @Option(
      names = {"--print-user-role"},
      description =
          "Print the SQL granting the application role (--app-role) access to DBOS system tables instead of executing it")
  boolean printUserRole;

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Spec CommandSpec spec;

  static final String[] GRANT_QUERIES = {
    "GRANT USAGE ON SCHEMA \"%1$s\" TO \"%2$s\"",
    "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA \"%1$s\" TO \"%2$s\"",
    "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA \"%1$s\" TO \"%2$s\"",
    "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA \"%1$s\" TO \"%2$s\"",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA \"%1$s\" GRANT ALL ON TABLES TO \"%2$s\"",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA \"%1$s\" GRANT ALL ON SEQUENCES TO \"%2$s\"",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA \"%1$s\" GRANT EXECUTE ON FUNCTIONS TO \"%2$s\""
  };

  @Override
  public Integer call() throws Exception {
    var out = spec.commandLine().getOut();

    if (printMigrations != null || printUserRole) {
      var exitCode = printSql(out, spec.commandLine().getErr());
      out.flush();
      return exitCode;
    }

    out.println("Starting DBOS migrations");
    out.format("  System Database: %s\n", dbOptions.url());
    out.format("  System Database User: %s\n", dbOptions.user());

    MigrationManager.runMigrations(
        dbOptions.url(),
        dbOptions.user(),
        dbOptions.password(),
        dbOptions.schema(),
        useListenNotify);
    grantDBOSSchemaPermissions(out, dbOptions.schema());
    return 0;
  }

  // Stdout stays pure SQL and comments (pipeable to a .sql file); never connects.
  int printSql(PrintWriter out, PrintWriter err) {
    if (printMigrations != null && printUserRole) {
      err.println("--print-user-role cannot be combined with --print-migrations");
      return 1;
    }
    var schema = SystemDatabase.sanitizeSchema(dbOptions.schema());
    if (schema.contains("'") || schema.contains("\"")) {
      err.println("Schema names containing quotes are not supported");
      return 1;
    }

    if (printUserRole) {
      if (appRole == null || appRole.isEmpty()) {
        err.println("--print-user-role requires --app-role");
        return 1;
      }
      if (appRole.contains("'") || appRole.contains("\"")) {
        err.println("Role names containing quotes are not supported");
        return 1;
      }
      out.format("-- Permissions on DBOS schema %s for role %s%n", schema, appRole);
      for (var query : GRANT_QUERIES) {
        out.println(query.formatted(schema, appRole) + ";");
      }
      return 0;
    }

    var latest = MigrationManager.getMigrations(schema, useListenNotify, false).size();
    int start;
    if (printMigrations.equals("all")) {
      start = 1;
    } else {
      try {
        start = Integer.parseInt(printMigrations);
      } catch (NumberFormatException e) {
        err.format(
            "Invalid --print-migrations value '%s': expected 'all' or a migration number%n",
            printMigrations);
        return 1;
      }
      if (start < 1 || start > latest) {
        err.format(
            "Migration %d does not exist: valid migrations are 1 through %d%n", start, latest);
        return 1;
      }
    }

    out.format("-- DBOS system database migrations for %s%n", maskPassword(dbOptions.url()));
    out.println(
        "-- Contains CREATE/DROP INDEX CONCURRENTLY: run outside a transaction block (e.g. plain psql, not psql --single-transaction).");
    out.print(MigrationManager.generateMigrationScript(schema, useListenNotify, start));
    return 0;
  }

  static String maskPassword(String url) {
    if (url == null) {
      return "the system database";
    }
    return url.replaceAll("(?i)(password=)[^&]*", "$1***");
  }

  void grantDBOSSchemaPermissions(PrintWriter out, String schema) throws SQLException {

    if (appRole == null || appRole.isEmpty()) {
      return;
    }
    schema = SystemDatabase.sanitizeSchema(schema);

    out.format(
        "Granting permissions for the %s schema to %s in database %s\n",
        schema, appRole, dbOptions.url());

    try (var conn =
            DriverManager.getConnection(dbOptions.url(), dbOptions.user(), dbOptions.password());
        var stmt = conn.createStatement()) {
      for (var query : GRANT_QUERIES) {
        query = query.formatted(schema, appRole);
        stmt.execute(query);
      }
    }
  }
}
