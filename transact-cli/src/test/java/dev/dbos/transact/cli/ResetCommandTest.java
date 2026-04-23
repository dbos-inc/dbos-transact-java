package dev.dbos.transact.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import picocli.CommandLine;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ResetCommandTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @BeforeEach
  public void setup() throws SQLException {
    var sql = "CREATE TABLE dummy_table (id SERIAL PRIMARY KEY, name VARCHAR(100));";
    try (var conn = pgContainer.connection();
        var stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  boolean dummyTableExists() throws SQLException {
    try (Connection conn = pgContainer.connection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getTables(null, null, "dummy_table", new String[] {"TABLE"})) {
        return rs.next();
      }
    }
  }

  @Test
  public void reset() throws Exception {

    assertTrue(dummyTableExists());

    var cmd = new CommandLine(new DBOSCommand());
    var sw = new StringWriter();
    cmd.setOut(new PrintWriter(sw));

    var args =
        Stream.of(List.of("reset"), pgContainer.options(), List.of("-y"))
            .flatMap(Collection::stream)
            .toArray(String[]::new);
    var exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    assertFalse(dummyTableExists());
  }
}
