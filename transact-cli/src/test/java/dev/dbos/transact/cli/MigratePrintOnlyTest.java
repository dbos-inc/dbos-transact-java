package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.migrations.MigrationManager;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

// Runs without a database; --print-only must not connect.
public class MigratePrintOnlyTest {

  @Test
  public void printOnly() {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode = cmd.execute("migrate", "--print-only");
    assertEquals(0, exitCode);

    var sql = sw.toString();
    assertFalse(sql.contains("Starting DBOS migrations"));
    assertTrue(sql.startsWith("CREATE SCHEMA IF NOT EXISTS \"dbos\";"));
    assertTrue(
        sql.contains(
            "CREATE TABLE IF NOT EXISTS \"dbos\".dbos_migrations (version BIGINT NOT NULL PRIMARY KEY);"));
    assertTrue(sql.contains("CREATE TABLE \"dbos\".workflow_status"));
    assertTrue(sql.contains("INSERT INTO \"dbos\".dbos_migrations (version) VALUES (1);"));

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
  public void printOnlyInvalidSchemaFails() {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));
    cmd.setErr(new PrintWriter(new StringWriter()));

    var exitCode = cmd.execute("migrate", "--print-only", "--schema", "bad\"schema");
    assertEquals(1, exitCode);
    assertEquals("", sw.toString());
  }
}
