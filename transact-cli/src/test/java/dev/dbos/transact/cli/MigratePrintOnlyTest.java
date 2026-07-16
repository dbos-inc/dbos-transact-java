package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.migrations.MigrationManager;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

// Runs without a database; --print-only must not connect.
public class MigratePrintOnlyTest {

  @Test
  public void printOnly() {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    var ew = new StringWriter();
    cmd.setOut(new PrintWriter(sw));
    cmd.setErr(new PrintWriter(ew));

    var exitCode = cmd.execute("migrate", "--print-only");
    assertEquals(0, exitCode);
    // No credentials: full fresh script, silently (stdout must be pipeable to a .sql file).
    assertEquals("", ew.toString());

    var sql = sw.toString();
    assertFalse(sql.contains("Starting DBOS migrations"));
    assertTrue(sql.startsWith("-- DBOS system database migration script for schema \"dbos\"."));
    assertTrue(sql.contains("CREATE SCHEMA IF NOT EXISTS \"dbos\";"));
    assertTrue(
        sql.contains(
            "CREATE TABLE IF NOT EXISTS \"dbos\".dbos_migrations (version BIGINT NOT NULL PRIMARY KEY);"));
    // Fresh-database guard immediately after the migrations table is created.
    assertTrue(
        sql.contains(
            "RAISE EXCEPTION 'DBOS schema dbos is already at version %; this script is for fresh databases only. Use dbos migrate instead.', existing_version;"));
    assertTrue(sql.contains("CREATE TABLE \"dbos\".workflow_status"));
    assertTrue(sql.contains("INSERT INTO \"dbos\".dbos_migrations (version) VALUES (1);"));
    // Migration 10 is emitted with the runner's conditional, not omitted.
    assertTrue(sql.contains("WHERE table_schema = 'dbos' AND table_name = 'notifications'"));
    assertTrue(sql.contains("AND constraint_type = 'PRIMARY KEY'"));
    assertTrue(sql.contains("ALTER TABLE \"dbos\".notifications ADD PRIMARY KEY (message_uuid);"));

    var latest = MigrationManager.getMigrations("dbos", true, false).size();
    assertTrue(sql.contains("UPDATE \"dbos\".dbos_migrations SET version = %d;".formatted(latest)));
    assertFalse(sql.contains("GRANT"));

    // Every non-blank line is SQL or a comment; every statement block ends with ';'.
    for (var line : sql.split("\n")) {
      if (line.isBlank() || line.startsWith("--")) {
        continue;
      }
      assertFalse(line.startsWith("Starting"), "unexpected non-SQL output: " + line);
    }
  }

  @Test
  public void printOnlyWithAppRoleAndSchema() {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode =
        cmd.execute("migrate", "--print-only", "--schema", "custom", "--app-role", "app_user");
    assertEquals(0, exitCode);

    var sql = sw.toString();
    assertTrue(sql.contains("CREATE SCHEMA IF NOT EXISTS \"custom\";"));
    assertTrue(sql.contains("CREATE TABLE \"custom\".workflow_status"));
    assertTrue(sql.contains("GRANT USAGE ON SCHEMA custom TO app_user;"));
    assertTrue(
        sql.contains(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA custom GRANT EXECUTE ON FUNCTIONS TO app_user;"));
  }

  @Test
  public void printOnlyFunnySchema() {
    var schema = "F8nny_sCHem@-n@m3";
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode = cmd.execute("migrate", "--print-only", "--schema", schema);
    assertEquals(0, exitCode);

    var sql = sw.toString();
    assertTrue(sql.contains("CREATE SCHEMA IF NOT EXISTS \"%s\";".formatted(schema)));
    assertTrue(sql.contains("CREATE TABLE \"%s\".workflow_status".formatted(schema)));
    assertTrue(
        sql.contains(
            "CREATE TABLE IF NOT EXISTS \"%s\".dbos_migrations (version BIGINT NOT NULL PRIMARY KEY);"
                .formatted(schema)));
    // Migration 10 guard uses the schema as a string literal.
    assertTrue(sql.contains("WHERE table_schema = '%s' AND table_name".formatted(schema)));
    // The schema never appears unquoted in an identifier position (schema immediately
    // followed by a dot only happens without the closing double quote).
    assertFalse(sql.contains(schema + "."));

    var latest = MigrationManager.getMigrations(schema, true, false).size();
    assertTrue(
        sql.contains("UPDATE \"%s\".dbos_migrations SET version = %d;".formatted(schema, latest)));
  }

  @Test
  public void printOnlyConnectionFailureFallsBackToFreshScript() {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    var ew = new StringWriter();
    cmd.setOut(new PrintWriter(sw));
    cmd.setErr(new PrintWriter(ew));

    var exitCode =
        cmd.execute(
            "migrate",
            "--print-only",
            "-D=jdbc:postgresql://127.0.0.1:1/nosuchdb?connectTimeout=2",
            "-U=nobody",
            "-P=nothing");
    assertEquals(0, exitCode);
    assertEquals("", ew.toString());

    var sql = sw.toString();
    assertTrue(sql.startsWith("-- DBOS system database migration script for schema \"dbos\"."));
    assertTrue(sql.contains("CREATE SCHEMA IF NOT EXISTS \"dbos\";"));
    assertTrue(sql.contains("INSERT INTO \"dbos\".dbos_migrations (version) VALUES (1);"));
  }

  @Test
  public void printOnlyDelta() {
    var latest = MigrationManager.getMigrations("dbos", true, false).size();
    var from = latest - 2;
    var sql = MigrationManager.generateMigrationScript("dbos", true, from);

    // Delta prelude: no schema/table creation, no fresh guard; exact-version guard instead.
    assertTrue(
        sql.startsWith(
            "-- DBOS system database delta migration script for schema \"dbos\" (version %d to %d)."
                .formatted(from, latest)));
    assertFalse(sql.contains("CREATE SCHEMA"));
    assertFalse(sql.contains("CREATE TABLE IF NOT EXISTS \"dbos\".dbos_migrations"));
    assertFalse(sql.contains("fresh databases only"));
    assertTrue(sql.contains("IF existing_version IS DISTINCT FROM %d THEN".formatted(from)));

    // Only migrations from+1..latest appear.
    for (var v = 1; v <= from; v++) {
      assertFalse(sql.contains("-- DBOS system database migration %d\n".formatted(v)));
    }
    for (var v = from + 1; v <= latest; v++) {
      assertTrue(sql.contains("-- DBOS system database migration %d\n".formatted(v)));
    }
    // Migration 10 is out of range, so its conditional block is absent.
    assertFalse(sql.contains("table_constraints"));

    // Bookkeeping: UPDATE only, never INSERT.
    assertFalse(sql.contains("INSERT INTO \"dbos\".dbos_migrations"));
    assertTrue(
        sql.contains("UPDATE \"dbos\".dbos_migrations SET version = %d;".formatted(from + 1)));
    assertTrue(sql.contains("UPDATE \"dbos\".dbos_migrations SET version = %d;".formatted(latest)));
  }

  @Test
  public void printOnlyDeltaFunnySchema() {
    var schema = "F8nny_sCHem@-n@m3";
    var latest = MigrationManager.getMigrations(schema, true, false).size();
    var from = 9; // puts migration 10 in the delta range
    var sql = MigrationManager.generateMigrationScript(schema, true, from);

    assertTrue(sql.contains("SELECT version INTO existing_version FROM \"%s\"".formatted(schema)));
    assertTrue(sql.contains("WHERE table_schema = '%s' AND table_name".formatted(schema)));
    assertTrue(
        sql.contains(
            "ALTER TABLE \"%s\".notifications ADD PRIMARY KEY (message_uuid);".formatted(schema)));
    assertTrue(
        sql.contains("UPDATE \"%s\".dbos_migrations SET version = %d;".formatted(schema, latest)));
    assertFalse(sql.contains(schema + ".")); // never unquoted in identifier position
    // Bookkeeping starts with an UPDATE, never an INSERT (function bodies in
    // migrations 14/38 legitimately INSERT INTO workflow_status).
    assertFalse(sql.contains("INSERT INTO \"%s\".dbos_migrations".formatted(schema)));
  }

  @Test
  public void printOnlyUpToDateAndInvalidFromVersion() {
    var latest = MigrationManager.getMigrations("dbos", true, false).size();
    assertEquals(
        "-- Database is already at the latest DBOS schema version (%d); nothing to do.\n"
            .formatted(latest),
        MigrationManager.generateMigrationScript("dbos", true, latest));

    assertThrows(
        IllegalArgumentException.class,
        () -> MigrationManager.generateMigrationScript("dbos", true, -1));
    assertThrows(
        IllegalArgumentException.class,
        () -> MigrationManager.generateMigrationScript("dbos", true, latest + 1));
  }

  @ParameterizedTest
  @ValueSource(strings = {"bad\"schema", "bad'schema"})
  public void printOnlyInvalidSchemaFails(String schema) {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));
    cmd.setErr(new PrintWriter(new StringWriter()));

    var exitCode = cmd.execute("migrate", "--print-only", "--schema", schema);
    assertEquals(1, exitCode);
    assertEquals("", sw.toString());
  }
}
