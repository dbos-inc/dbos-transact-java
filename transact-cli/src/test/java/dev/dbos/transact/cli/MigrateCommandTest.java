package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;

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
