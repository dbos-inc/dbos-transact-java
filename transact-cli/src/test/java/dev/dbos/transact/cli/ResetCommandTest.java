package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.migrations.MigrationManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import picocli.CommandLine;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ResetCommandTest {

  static String db_url = "jdbc:postgresql://localhost:5432/reset_cmd_test";
  static String db_user = Objects.requireNonNullElse(System.getenv("PGUSER"), "postgres");
  static String db_password = Objects.requireNonNullElse(System.getenv("PGPASSWORD"), "dbos");

  @BeforeAll
  static void setup() throws Exception {

    var pair = MigrationManager.extractDbAndPostgresUrl(db_url);
    var dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", pair.database());
    var createDbSql = String.format("CREATE DATABASE %s", pair.database());
    try (var conn = DriverManager.getConnection(pair.url(), db_user, db_password);
        var stmt = conn.createStatement()) {
      stmt.execute(dropDbSql);
      stmt.execute(createDbSql);
    }

    var createDummyTableSql =
        "CREATE TABLE dummy_table (id SERIAL PRIMARY KEY, name VARCHAR(100));";
    try (var conn = DriverManager.getConnection(db_url, db_user, db_password);
        var stmt = conn.createStatement()) {
      stmt.execute(createDummyTableSql);
    }
  }

  static boolean dummyTableExists(String url, String user, String password) throws SQLException {
    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getTables(null, null, "dummy_table", new String[] {"TABLE"})) {
        return rs.next();
      }
    }
  }

  @Test
  public void reset() throws Exception {

    assertTrue(dummyTableExists(db_url, db_user, db_password));

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode = cmd.execute("reset", "-D=" + db_url, "-U=" + db_user, "-y");
    assertEquals(0, exitCode);

    assertFalse(dummyTableExists(db_url, db_user, db_password));
  }
}
