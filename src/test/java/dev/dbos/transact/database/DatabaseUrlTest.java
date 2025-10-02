package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class DatabaseUrlTest {

  @Test
  public void parseJdbcUrl() {
    var uri = "jdbc:postgresql://username:password@localhost:5432/dbos_java_sys";

    var dbinfo = DatabaseUrl.fromUri(uri);
    assertEquals("postgresql", dbinfo.scheme());
    assertEquals("username", dbinfo.username());
    assertEquals("password", dbinfo.password());
    assertEquals("localhost", dbinfo.host());
    assertEquals(5432, dbinfo.port());
    assertEquals("dbos_java_sys", dbinfo.database());
    assertEquals(0, dbinfo.query().size());
  }

  @Test
  public void parseJdbcUrlNoUserNameOrPassword() {
    var uri = "jdbc:postgresql://localhost:5432/dbos_java_sys";

    var dbinfo = DatabaseUrl.fromUri(uri);
    assertEquals("postgresql", dbinfo.scheme());
    assertEquals(null, dbinfo.username());
    assertEquals(null, dbinfo.password());
    assertEquals("localhost", dbinfo.host());
    assertEquals(5432, dbinfo.port());
    assertEquals("dbos_java_sys", dbinfo.database());
    assertEquals(0, dbinfo.query().size());
  }

  @Test
  public void parseJdbcUrlWithQuery() {
    var uri = "jdbc:postgresql://username:password@localhost:5432/dbos_java_sys?ssl=true";

    var dbinfo = DatabaseUrl.fromUri(uri);
    assertEquals("postgresql", dbinfo.scheme());
    assertEquals("username", dbinfo.username());
    assertEquals("password", dbinfo.password());
    assertEquals("localhost", dbinfo.host());
    assertEquals(5432, dbinfo.port());
    assertEquals("dbos_java_sys", dbinfo.database());
    assertTrue(dbinfo.query().containsKey("ssl"));
    assertEquals("true", dbinfo.query().get("ssl"));
  }

  @Test
  public void parseJdbcUrlWithQueryUserAndPassword() {
    var uri =
        "jdbc:postgresql://username:password@localhost:5432/dbos_java_sys?user=username1&password=password1";

    var dbinfo = DatabaseUrl.fromUri(uri);
    assertEquals("postgresql", dbinfo.scheme());
    assertEquals("username1", dbinfo.username());
    assertEquals("password1", dbinfo.password());
    assertEquals("localhost", dbinfo.host());
    assertEquals(5432, dbinfo.port());
    assertEquals("dbos_java_sys", dbinfo.database());
    assertEquals(0, dbinfo.query().size());
  }

  @Test
  public void parseJdbcUrlWithQueryUserOnly() {
    var uri = "jdbc:postgresql://username@localhost:5432/dbos_java_sys";

    var dbinfo = DatabaseUrl.fromUri(uri);
    assertEquals("postgresql", dbinfo.scheme());
    assertEquals("username", dbinfo.username());
    assertEquals(null, dbinfo.password());
    assertEquals("localhost", dbinfo.host());
    assertEquals(5432, dbinfo.port());
    assertEquals("dbos_java_sys", dbinfo.database());
    assertEquals(0, dbinfo.query().size());
  }

  @Test
  public void parsePgUrl() {
    var uri = "postgresql://username:password@localhost:5432/dbos_java_sys";

    var dbinfo = DatabaseUrl.fromUri(uri);
    assertEquals("postgresql", dbinfo.scheme());
    assertEquals("username", dbinfo.username());
    assertEquals("password", dbinfo.password());
    assertEquals("localhost", dbinfo.host());
    assertEquals(5432, dbinfo.port());
    assertEquals("dbos_java_sys", dbinfo.database());
    assertEquals(0, dbinfo.query().size());
  }

  @Test
  public void parsePgUrlWithQuery() {
    var uri = "postgresql://username:password@localhost:5432/dbos_java_sys?ssl=true";

    var dbinfo = DatabaseUrl.fromUri(uri);
    assertEquals("postgresql", dbinfo.scheme());
    assertEquals("username", dbinfo.username());
    assertEquals("password", dbinfo.password());
    assertEquals("localhost", dbinfo.host());
    assertEquals(5432, dbinfo.port());
    assertEquals("dbos_java_sys", dbinfo.database());
    assertEquals("true", dbinfo.query().get("ssl"));
  }

  @Test
  public void parsePgUrlWithQueryUserAndPassword() {
    var uri =
        "postgresql://username:password@localhost:5432/dbos_java_sys?user=username1&password=password1";

    var dbinfo = DatabaseUrl.fromUri(uri);
    assertEquals("postgresql", dbinfo.scheme());
    assertEquals("username", dbinfo.username());
    assertEquals("password", dbinfo.password());
    assertEquals("localhost", dbinfo.host());
    assertEquals(5432, dbinfo.port());
    assertEquals("dbos_java_sys", dbinfo.database());
    assertEquals("username1", dbinfo.query().get("user"));
    assertEquals("password1", dbinfo.query().get("password"));
  }

  @Test
  public void requiredFields() {
    assertThrows(
        NullPointerException.class,
        () -> new DatabaseUrl(null, "host", 12345, "db", "user", "pwd", new HashMap<>()));
    assertThrows(
        NullPointerException.class,
        () -> new DatabaseUrl("scheme", null, 12345, "db", "user", "pwd", new HashMap<>()));
    assertThrows(
        NullPointerException.class,
        () -> new DatabaseUrl("scheme", "host", 12345, null, "user", "pwd", new HashMap<>()));
  }

  @Test
  public void toJdbcUrl() {
    var dbInfo =
        new DatabaseUrl("postgresql", "some-host", 12345, "some-db", "some-user", "some-pwd", null);
    var uri = dbInfo.jdbcUrl();
    assertEquals("jdbc:postgresql://some-host:12345/some-db", uri);
    assertEquals("some-user", dbInfo.username());
    assertEquals("some-pwd", dbInfo.password());
  }
}
