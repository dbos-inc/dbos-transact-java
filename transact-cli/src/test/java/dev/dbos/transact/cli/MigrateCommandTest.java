package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.DriverManager;
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
  public void migrate_print_migrations_apply_funny_schema() throws Exception {
    var schema = "F8nny_sCHem@-n@m3";

    var script = runPrint("--schema", schema, "--print-migrations", "all");
    assertTrue(script.contains("-- Migration 10 skipped: not applicable on fresh databases"));
    assertFalse(script.contains("ADD PRIMARY KEY (message_uuid)"));
    assertFalse(script.contains("DO $$"));

    // Apply the printed script to a fresh database with psql ON_ERROR_STOP.
    var applied = pgContainer.execPsql(script);
    assertEquals(0, applied.getExitCode(), applied.getStderr());

    assertTrue(checkTable(schema, "dbos_migrations"));
    assertTrue(checkTable(schema, "workflow_status"));
    assertTrue(checkTable(schema, "notifications"));

    var latest = MigrationManager.getMigrations(schema, true, false).size();
    assertEquals(latest, currentVersion(schema));

    // A real migration run now considers the database up to date.
    var cmd = new CommandLine(new DBOSCommand());
    cmd.setOut(new PrintWriter(new StringWriter()));
    var args =
        Stream.of(List.of("migrate", "--schema", schema), pgContainer.options())
            .flatMap(Collection::stream)
            .toArray(String[]::new);
    assertEquals(0, cmd.execute(args));
    assertEquals(latest, currentVersion(schema));
  }

  @Test
  public void migrate_print_from_migration() throws Exception {
    var schema = Constants.DB_SCHEMA;
    var latest = MigrationManager.getMigrations(schema, true, false).size();

    // Bring a fresh database to version latest-1 by truncating the full script.
    var full = runPrint("--print-migrations", "all");
    var marker = "UPDATE \"%s\".dbos_migrations SET version = %d;".formatted(schema, latest - 1);
    var idx = full.indexOf(marker);
    assertTrue(idx > 0);
    var appliedPartial = pgContainer.execPsql(full.substring(0, idx + marker.length()) + "\n");
    assertEquals(0, appliedPartial.getExitCode(), appliedPartial.getStderr());
    assertEquals(latest - 1, currentVersion(schema));

    // The last migration printed alone applies on top of version latest-1.
    var single = runPrint("--print-migrations", String.valueOf(latest));
    assertFalse(single.contains("CREATE SCHEMA"));
    assertFalse(single.contains("DO $$"));
    assertFalse(single.contains("INSERT INTO \"%s\".dbos_migrations".formatted(schema)));
    var applied = pgContainer.execPsql(single);
    assertEquals(0, applied.getExitCode(), applied.getStderr());
    assertEquals(latest, currentVersion(schema));
  }

  @Test
  public void migrate_print_user_role_grants_access() throws Exception {
    var schema = "F8nny_sCHem@-n@m3";
    var role = "my-app-role";

    var script = runPrint("--schema", schema, "--print-migrations", "all");
    var roleScript = runPrint("--schema", schema, "--print-user-role", "--app-role", role);
    assertTrue(
        roleScript.contains("GRANT USAGE ON SCHEMA \"%s\" TO \"%s\";".formatted(schema, role)));
    for (var line : roleScript.split("\n")) {
      assertTrue(
          line.startsWith("--") || line.startsWith("GRANT") || line.startsWith("ALTER"),
          "unexpected output: " + line);
    }

    try (var conn = pgContainer.connection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP ROLE IF EXISTS \"%s\"".formatted(role));
      stmt.execute("CREATE ROLE \"%s\" LOGIN PASSWORD 'app_role_pwd'".formatted(role));
    }
    try {
      for (var s : List.of(script, roleScript)) {
        var applied = pgContainer.execPsql(s);
        assertEquals(0, applied.getExitCode(), applied.getStderr());
      }

      // The app role can query the DBOS schema.
      var latest = MigrationManager.getMigrations(schema, true, false).size();
      try (var conn = DriverManager.getConnection(pgContainer.jdbcUrl(), role, "app_role_pwd");
          var stmt = conn.createStatement();
          var rs =
              stmt.executeQuery("SELECT version FROM \"%s\".dbos_migrations".formatted(schema))) {
        assertTrue(rs.next());
        assertEquals(latest, rs.getInt(1));
      }
    } finally {
      try (var conn = pgContainer.connection();
          var stmt = conn.createStatement()) {
        stmt.execute("DROP OWNED BY \"%s\"".formatted(role));
        stmt.execute("DROP ROLE \"%s\"".formatted(role));
      }
    }
  }

  String runPrint(String... printArgs) {
    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    var ew = new StringWriter();
    cmd.setOut(new PrintWriter(sw));
    cmd.setErr(new PrintWriter(ew));

    var args =
        Stream.of(List.of("migrate"), List.of(printArgs), pgContainer.options())
            .flatMap(Collection::stream)
            .toArray(String[]::new);

    assertEquals(0, cmd.execute(args), ew.toString());
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
