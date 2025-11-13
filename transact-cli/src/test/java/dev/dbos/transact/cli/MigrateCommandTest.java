package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.migrations.MigrationManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import picocli.CommandLine;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MigrateCommandTest {

  static String db_url = "jdbc:postgresql://localhost:5432/migrate_cmd_test";
  static String db_user = Objects.requireNonNullElse(System.getenv("PGUSER"), "postgres");
  static String db_password = Objects.requireNonNullElse(System.getenv("PGPASSWORD"), "dbos");

  @BeforeAll
  static void setup() throws Exception {
    var pair = MigrationManager.extractDbAndPostgresUrl(db_url);
    var dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", pair.database());
    try (var conn = DriverManager.getConnection(pair.url(), db_user, db_password);
        var stmt = conn.createStatement()) {
      stmt.execute(dropDbSql);
    }
  }

  @Test
  @Order(1)
  public void migrate() throws Exception {

    assertFalse(checkConnection(db_url, db_user, db_password));

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode = cmd.execute("migrate", "-D=" + db_url, "-U=" + db_user);
    assertEquals(0, exitCode);

    assertTrue(checkConnection(db_url, db_user, db_password));
  }

  @Test
  @Order(2)
  public void migrate_again() throws Exception {

    assertTrue(checkConnection(db_url, db_user, db_password));

    var app = new DBOSCommand();
    var cmd = new CommandLine(app);

    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode = cmd.execute("migrate", "-D=" + db_url, "-U=" + db_user);
    assertEquals(0, exitCode);

    assertTrue(checkConnection(db_url, db_user, db_password));
  }

  static boolean checkConnection(String url, String user, String password) {
    try (var conn = DriverManager.getConnection(url, user, password);
        var stmt = conn.createStatement()) {
      stmt.execute("SELECT 1");
      return true;
    } catch (SQLException e) {
      return false;
    }
  }
}
