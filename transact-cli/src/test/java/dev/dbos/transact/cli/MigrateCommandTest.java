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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class MigrateCommandTest {

  static String db_url = "jdbc:postgresql://localhost:5432/migrate_cmd_test";
  static String db_user = Objects.requireNonNullElse(System.getenv("PGUSER"), "postgres");
  static String db_password = Objects.requireNonNullElse(System.getenv("PGPASSWORD"), "dbos");

  @BeforeEach
  public void setup() throws Exception {
    var pair = MigrationManager.extractDbAndPostgresUrl(db_url);
    var dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", pair.database());
    try (var conn = DriverManager.getConnection(pair.url(), db_user, db_password);
        var stmt = conn.createStatement()) {
      stmt.execute(dropDbSql);
    }
  }

  @Test
  public void migrate() throws Exception {

    assertFalse(checkConnection());

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode = cmd.execute("migrate", "-D=" + db_url, "-U=" + db_user);
    assertEquals(0, exitCode);

    assertTrue(checkConnection());
    assertTrue(checkTable(Constants.DB_SCHEMA, "workflow_status"));
  }

  @Test
  public void migrate_twice() throws Exception {

    migrate();

    assertTrue(checkConnection());

    var app = new DBOSCommand();
    var cmd = new CommandLine(app);

    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode = cmd.execute("migrate", "-D=" + db_url, "-U=" + db_user);
    assertEquals(0, exitCode);

    assertTrue(checkConnection());
    assertTrue(checkTable(Constants.DB_SCHEMA, "workflow_status"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"invalid\"schema", "invalid'schema"})
  void testRunMigrations_fails_invalid_schema(String schema) throws Exception {
    assertFalse(checkConnection());

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode =
        cmd.execute("migrate", "-D=" + db_url, "-U=" + db_user, "--schema", "%s".formatted(schema));
    assertEquals(1, exitCode);
  }

  @ParameterizedTest
  @ValueSource(strings = {"F8nny_sCHem@-n@m3", "embedded\0null"})
  public void migrate_custom_schema(String schema) throws Exception {

    assertFalse(checkConnection());

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode =
        cmd.execute("migrate", "-D=" + db_url, "-U=" + db_user, "--schema", "%s".formatted(schema));
    assertEquals(0, exitCode);

    assertTrue(checkConnection());
    assertTrue(checkTable(schema, "workflow_status"));
  }

  static boolean checkConnection() {
    try (var conn = DriverManager.getConnection(db_url, db_user, db_password);
        var stmt = conn.createStatement()) {
      stmt.execute("SELECT 1");
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  static boolean checkTable(String schema, String table) throws SQLException {
    var sql =
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)";
    try (var conn = DriverManager.getConnection(db_url, db_user, db_password);
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
