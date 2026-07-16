package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

public class MigrateCommandTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @Test
  public void migrate() throws Exception {

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var args =
        Stream.of(List.of("migrate"), pgContainer.options())
            .flatMap(Collection::stream)
            .toArray(String[]::new);

    var exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    assertTrue(checkTable(Constants.DB_SCHEMA, "workflow_status"));
  }

  @Test
  public void migrate_twice() throws Exception {

    migrate();

    var app = new DBOSCommand();
    var cmd = new CommandLine(app);

    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var args =
        Stream.of(List.of("migrate"), pgContainer.options())
            .flatMap(Collection::stream)
            .toArray(String[]::new);

    var exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    assertTrue(checkTable(Constants.DB_SCHEMA, "workflow_status"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"invalid\"schema", "invalid'schema"})
  void testRunMigrations_fails_invalid_schema(String schema) throws Exception {

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var args =
        Stream.of(
                List.of("migrate"),
                pgContainer.options(),
                List.of("--schema", "%s".formatted(schema)))
            .flatMap(Collection::stream)
            .toArray(String[]::new);

    var exitCode = cmd.execute(args);
    assertEquals(1, exitCode);
  }

  @ParameterizedTest
  @ValueSource(strings = {"F8nny_sCHem@-n@m3", "embedded\0null"})
  public void migrate_custom_schema(String schema) throws Exception {

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var args =
        Stream.of(
                List.of("migrate"),
                pgContainer.options(),
                List.of("--schema", "%s".formatted(schema)))
            .flatMap(Collection::stream)
            .toArray(String[]::new);

    var exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    assertTrue(checkTable(schema, "workflow_status"));
  }

  @Test
  public void migrate_print_only_apply_funny_schema() throws Exception {
    var schema = "F8nny_sCHem@-n@m3";

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));
    var exitCode = cmd.execute("migrate", "--print-only", "--schema", schema);
    assertEquals(0, exitCode);
    var script = sw.toString();

    // Apply the printed script to a fresh database with psql ON_ERROR_STOP.
    var applied = pgContainer.execPsql(script);
    assertEquals(0, applied.getExitCode(), applied.getStderr());

    assertTrue(checkTable(schema, "dbos_migrations"));
    assertTrue(checkTable(schema, "workflow_status"));
    assertTrue(checkTable(schema, "notifications"));

    var latest = MigrationManager.getMigrations(schema, true, false).size();
    try (var conn = pgContainer.connection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery("SELECT version FROM \"%s\".dbos_migrations".formatted(schema))) {
      assertTrue(rs.next());
      assertEquals(latest, rs.getInt(1));
      assertFalse(rs.next());
    }

    // Re-applying must abort immediately: the script is for fresh databases only.
    var reapplied = pgContainer.execPsql(script);
    assertNotEquals(0, reapplied.getExitCode());
    assertTrue(
        reapplied.getStderr().contains("this script is for fresh databases only"),
        reapplied.getStderr());
  }

  @ParameterizedTest
  @ValueSource(strings = {Constants.DB_SCHEMA, "F8nny_sCHem@-n@m3"})
  public void migrate_print_only_delta(String schema) throws Exception {
    var latest = MigrationManager.getMigrations(schema, true, false).size();
    var from = latest - 3;

    // Build a genuinely partial database at version `from` by applying the fresh
    // script only up through migration `from`'s bookkeeping.
    var fresh = MigrationManager.generateMigrationScript(schema, true);
    var marker = "\n-- DBOS system database migration %d\n".formatted(from + 1);
    var idx = fresh.indexOf(marker);
    assertTrue(idx > 0);
    var partial = pgContainer.execPsql(fresh.substring(0, idx));
    assertEquals(0, partial.getExitCode(), partial.getStderr());
    assertEquals(from, currentVersion(schema));

    // The CLI connects, reads the version, and prints a delta.
    var delta = runPrintOnly(schema);
    assertTrue(delta.contains("IF existing_version IS DISTINCT FROM %d THEN".formatted(from)));
    assertFalse(delta.contains("CREATE SCHEMA"));
    assertFalse(delta.contains("INSERT INTO \"%s\".dbos_migrations".formatted(schema)));
    assertFalse(delta.contains("-- DBOS system database migration %d\n".formatted(from)));
    assertTrue(delta.contains("-- DBOS system database migration %d\n".formatted(from + 1)));

    var applied = pgContainer.execPsql(delta);
    assertEquals(0, applied.getExitCode(), applied.getStderr());
    assertEquals(latest, currentVersion(schema));

    // Re-applying the delta fails the exact-version guard under ON_ERROR_STOP.
    var reapplied = pgContainer.execPsql(delta);
    assertNotEquals(0, reapplied.getExitCode());
    assertTrue(
        reapplied.getStderr().contains("upgrades from version %d".formatted(from)),
        reapplied.getStderr());
    assertEquals(latest, currentVersion(schema));

    // An up-to-date database prints only the nothing-to-do comment.
    assertEquals(
        "-- Database is already at the latest DBOS schema version (%d); nothing to do.\n"
            .formatted(latest),
        runPrintOnly(schema));

    // The fresh script against an already-migrated database fails fast.
    var freshOnMigrated = pgContainer.execPsql(fresh);
    assertNotEquals(0, freshOnMigrated.getExitCode());
    assertTrue(
        freshOnMigrated.getStderr().contains("this script is for fresh databases only"),
        freshOnMigrated.getStderr());
  }

  String runPrintOnly(String schema) {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    var ew = new StringWriter();
    cmd.setOut(new PrintWriter(sw));
    cmd.setErr(new PrintWriter(ew));

    var args =
        Stream.of(List.of("migrate", "--print-only", "--schema", schema), pgContainer.options())
            .flatMap(Collection::stream)
            .toArray(String[]::new);

    assertEquals(0, cmd.execute(args));
    assertEquals("", ew.toString());
    return sw.toString();
  }

  int currentVersion(String schema) throws SQLException {
    try (var conn = pgContainer.connection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery("SELECT version FROM \"%s\".dbos_migrations".formatted(schema))) {
      assertTrue(rs.next());
      var version = rs.getInt(1);
      assertFalse(rs.next());
      return version;
    }
  }

  boolean checkTable(String schema, String table) throws SQLException {
    var sql =
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)";
    try (var conn = pgContainer.connection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, SystemDatabase.sanitizeSchema(schema));
      stmt.setString(2, table);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getBoolean("exists");
        } else {
          return false;
        }
      }
    }
  }
}
