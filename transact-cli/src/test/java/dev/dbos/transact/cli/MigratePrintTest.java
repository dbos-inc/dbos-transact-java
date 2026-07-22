package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.migrations.MigrationManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

// Runs without a database; the print flags must never connect.
public class MigratePrintTest {

  record Result(int exitCode, String out, String err) {}

  static Result run(String... args) {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    var ew = new StringWriter();
    cmd.setOut(new PrintWriter(sw));
    cmd.setErr(new PrintWriter(ew));
    var exitCode = cmd.execute(args);
    return new Result(exitCode, sw.toString(), ew.toString());
  }

  @Test
  public void printMigrationsAll() {
    var r = run("migrate", "--print-migrations", "all");
    assertEquals(0, r.exitCode());
    // Stdout is pure SQL and comments, pipeable to a .sql file.
    assertEquals("", r.err());

    var sql = r.out();
    var latest = MigrationManager.getMigrations("dbos", true, false).size();
    assertTrue(sql.startsWith("-- DBOS system database migrations for "));
    assertTrue(
        sql.contains(
            "-- Contains CREATE/DROP INDEX CONCURRENTLY: run outside a transaction block (e.g. plain psql, not psql --single-transaction)."));
    assertTrue(sql.contains("-- This script is for FRESH databases only."));
    assertTrue(sql.contains("CREATE SCHEMA IF NOT EXISTS \"dbos\";"));
    assertTrue(
        sql.contains(
            "CREATE TABLE IF NOT EXISTS \"dbos\".dbos_migrations (version BIGINT NOT NULL PRIMARY KEY);"));
    assertTrue(sql.contains("CREATE TABLE \"dbos\".workflow_status"));
    assertTrue(sql.contains("INSERT INTO \"dbos\".dbos_migrations (version) VALUES (1);"));
    assertTrue(sql.contains("-- Migration 10 skipped: not applicable on fresh databases"));
    assertFalse(sql.contains("ADD PRIMARY KEY (message_uuid)"));
    assertTrue(sql.contains("UPDATE \"dbos\".dbos_migrations SET version = 10;"));
    assertTrue(sql.contains("UPDATE \"dbos\".dbos_migrations SET version = %d;".formatted(latest)));
    assertFalse(sql.contains("DO $$"));
    // Role grants are only printed by --print-user-role
    assertFalse(sql.contains("GRANT"));
    for (var line : sql.split("\n")) {
      assertFalse(line.startsWith("Starting"), "unexpected non-SQL output: " + line);
      assertFalse(line.startsWith("Granting"), "unexpected non-SQL output: " + line);
    }
  }

  @Test
  public void printMigrationsFromNumber() {
    // Starting from 1 is identical to "all".
    assertEquals(
        run("migrate", "--print-migrations", "all").out(),
        run("migrate", "--print-migrations", "1").out());

    // Starting mid-way omits the prelude and earlier migrations.
    var latest = MigrationManager.getMigrations("dbos", true, false).size();
    var r = run("migrate", "--print-migrations", "10");
    assertEquals(0, r.exitCode());
    assertEquals("", r.err());
    var out = r.out();
    assertFalse(out.contains("CREATE SCHEMA"));
    assertFalse(out.contains("-- Migration 9\n"));
    assertTrue(out.contains("-- Migration 10 skipped: not applicable on fresh databases"));
    assertTrue(out.contains("UPDATE \"dbos\".dbos_migrations SET version = 10;"));
    assertTrue(List.of(out.split("\n")).contains("-- Migration 11"));
    assertTrue(out.contains("UPDATE \"dbos\".dbos_migrations SET version = %d;".formatted(latest)));
    assertFalse(out.contains("INSERT INTO \"dbos\".dbos_migrations"));
    assertFalse(out.contains("DO $$"));
  }

  @Test
  public void printMigrationsInvalidValues() {
    var latest = MigrationManager.getMigrations("dbos", true, false).size();
    for (var bad : List.of("0", "-1", String.valueOf(latest + 1))) {
      var r = run("migrate", "--print-migrations", bad);
      assertEquals(1, r.exitCode());
      assertEquals("", r.out());
      assertTrue(
          r.err().contains("does not exist: valid migrations are 1 through %d".formatted(latest)),
          r.err());
    }

    var r = run("migrate", "--print-migrations", "nope");
    assertEquals(1, r.exitCode());
    assertEquals("", r.out());
    assertTrue(
        r.err()
            .contains(
                "Invalid --print-migrations value 'nope': expected 'all' or a migration number"),
        r.err());

    assertThrows(
        IllegalArgumentException.class,
        () -> MigrationManager.generateMigrationScript("dbos", true, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> MigrationManager.generateMigrationScript("dbos", true, latest + 1));
  }

  @Test
  public void printMigrationsFunnySchema() {
    var schema = "F8nny_sCHem@-n@m3";
    var r = run("migrate", "--print-migrations", "all", "--schema", schema);
    assertEquals(0, r.exitCode());
    assertEquals("", r.err());

    var sql = r.out();
    assertTrue(sql.contains("CREATE SCHEMA IF NOT EXISTS \"%s\";".formatted(schema)));
    assertTrue(sql.contains("CREATE TABLE \"%s\".workflow_status".formatted(schema)));
    // The schema never appears unquoted in an identifier position (schema immediately
    // followed by a dot only happens without the closing double quote).
    assertFalse(sql.contains(schema + "."));

    var latest = MigrationManager.getMigrations(schema, true, false).size();
    assertTrue(
        sql.contains("UPDATE \"%s\".dbos_migrations SET version = %d;".formatted(schema, latest)));
  }

  @Test
  public void printUserRole() {
    var r = run("migrate", "--print-user-role", "--schema", "custom", "--app-role", "app_user");
    assertEquals(0, r.exitCode());
    assertEquals("", r.err());

    var sql = r.out();
    assertTrue(sql.startsWith("-- Permissions on DBOS schema custom for role app_user"));
    assertTrue(sql.contains("GRANT USAGE ON SCHEMA \"custom\" TO \"app_user\";"));
    assertTrue(
        sql.contains(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA \"custom\" GRANT EXECUTE ON FUNCTIONS TO \"app_user\";"));
    for (var line : sql.split("\n")) {
      assertTrue(
          line.startsWith("--") || line.startsWith("GRANT") || line.startsWith("ALTER"),
          "unexpected output: " + line);
    }
  }

  @Test
  public void printUserRoleRequiresAppRole() {
    var r = run("migrate", "--print-user-role");
    assertEquals(1, r.exitCode());
    assertEquals("", r.out());
    assertTrue(r.err().contains("--print-user-role requires --app-role"), r.err());
  }

  @Test
  public void printFlagsAreMutuallyExclusive() {
    var r = run("migrate", "--print-migrations", "all", "--print-user-role", "-r", "app_user");
    assertEquals(1, r.exitCode());
    assertEquals("", r.out());
    assertTrue(
        r.err().contains("--print-user-role cannot be combined with --print-migrations"), r.err());
  }

  @ParameterizedTest
  @ValueSource(strings = {"bad\"schema", "bad'schema"})
  public void printInvalidSchemaFails(String schema) {
    var r = run("migrate", "--print-migrations", "all", "--schema", schema);
    assertEquals(1, r.exitCode());
    assertEquals("", r.out());
    assertTrue(r.err().contains("Schema names containing quotes are not supported"), r.err());

    r = run("migrate", "--print-user-role", "-r", "app_user", "--schema", schema);
    assertEquals(1, r.exitCode());
    assertEquals("", r.out());
    assertTrue(r.err().contains("Schema names containing quotes are not supported"), r.err());
  }

  @ParameterizedTest
  @ValueSource(strings = {"bad\"role", "bad'role"})
  public void printInvalidRoleFails(String role) {
    var r = run("migrate", "--print-user-role", "-r", role);
    assertEquals(1, r.exitCode());
    assertEquals("", r.out());
    assertTrue(r.err().contains("Role names containing quotes are not supported"), r.err());
  }
}
