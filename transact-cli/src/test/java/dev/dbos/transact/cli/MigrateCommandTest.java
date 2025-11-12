package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import picocli.CommandLine;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MigrateCommandTest {

  static String db_url = "jdbc:postgresql://localhost:5432/migrate_cmd_test";
  static String db_user = Objects.requireNonNullElse(System.getenv("PGUSER"), "postgres");
  static String db_password = Objects.requireNonNullElse(System.getenv("PGPASSWORD"), "dbos");

  @BeforeAll
  static void setup() throws Exception {
    DBUtils.dropDatabase(db_url, db_user, db_password);
  }

  @Test
  @Order(1)
  public void migrate() throws Exception {

    assertFalse(DBUtils.checkConnection(db_url, db_user, db_password));

    var app = new DBOSCommand();
    var cmd = new CommandLine(app);

    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode =
        cmd.execute(
            "migrate", "-D=jdbc:postgresql://localhost:5432/migrate_cmd_test", "-U=postgres");
    assertEquals(0, exitCode);

    assertTrue(DBUtils.checkConnection(db_url, db_user, db_password));
  }

  @Test
  @Order(2)
  public void migrate_again() throws Exception {

    assertTrue(DBUtils.checkConnection(db_url, db_user, db_password));

    var app = new DBOSCommand();
    var cmd = new CommandLine(app);

    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var exitCode =
        cmd.execute(
            "migrate", "-D=jdbc:postgresql://localhost:5432/migrate_cmd_test", "-U=postgres");
    assertEquals(0, exitCode);

    assertTrue(DBUtils.checkConnection(db_url, db_user, db_password));
  }
}
