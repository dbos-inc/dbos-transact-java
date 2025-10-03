package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class SystemDatabaseUrlTest {
  @Test
  public void foo() {
    var originalUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys?user=alice&ssl=true";
    var foo = org.postgresql.Driver.parseURL(originalUrl, null);
    var baz = doThePgThing(originalUrl);

    int i = 12;
  }

  record FooRec(String url, String database) {}

  static FooRec doThePgThing(String url) {
    int qm = url.indexOf('?');
    var base = qm >= 0 ? url.substring(0, qm) : url;
    var params = qm >= 0 ? url.substring(qm) : "";
    int slash = base.lastIndexOf('/');
    if (slash < "jdbc:postgresql://".length()) {
      throw new IllegalArgumentException();
    }

    var newUrl = base.substring(0, slash + 1) + "postgres" + params;
    var databaseName = base.substring(slash + 1);
    return new FooRec(newUrl, databaseName);
  }

  //   @Test
  //   public void testBasicUrlWithoutParameters() {
  //     String originalUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys";
  //     String expectedUrl = "jdbc:postgresql://localhost:5432/postgres";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }

  //   @Test
  //   @DisplayName("Should convert JDBC URL with multiple parameters")
  //   public void testUrlWithMultipleParameters() {
  //     String originalUrl =
  //         "jdbc:postgresql://localhost:5432/dbos_java_sys?user=admin&password=secret&ssl=true";
  //     String expectedUrl =
  //         "jdbc:postgresql://localhost:5432/postgres?user=admin&password=secret&ssl=true";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }

  //   @Test
  //   @DisplayName("Should handle URL without port number")
  //   public void testUrlWithoutPort() {
  //     String originalUrl = "jdbc:postgresql://localhost/mydb?user=test&password=pass";
  //     String expectedUrl = "jdbc:postgresql://localhost/postgres?user=test&password=pass";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }

  //   @Test
  //   @DisplayName("Should handle URL with remote host and custom port")
  //   public void testRemoteHostWithCustomPort() {
  //     String originalUrl =
  //
  // "jdbc:postgresql://db.example.com:5433/production?user=prod&password=secret&sslmode=require";
  //     String expectedUrl =
  //
  // "jdbc:postgresql://db.example.com:5433/postgres?user=prod&password=secret&sslmode=require";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }

  //   @Test
  //   @DisplayName("Should handle database name with underscores and numbers")
  //   public void testDatabaseNameWithSpecialCharacters() {
  //     String originalUrl = "jdbc:postgresql://localhost:5432/dbos_test_db_123?user=testuser";
  //     String expectedUrl = "jdbc:postgresql://localhost:5432/postgres?user=testuser";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }

  //   @Test
  //   @DisplayName("Should handle empty query parameters")
  //   public void testUrlWithEmptyQueryParameters() {
  //     String originalUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys?";
  //     String expectedUrl = "jdbc:postgresql://localhost:5432/postgres?";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }

  //   @Test
  //   @DisplayName("Should handle query parameters with special characters")
  //   public void testUrlWithSpecialCharactersInParameters() {
  //     String originalUrl =
  //         "jdbc:postgresql://localhost:5432/mydb?user=admin&password=p@ssw0rd!&ssl=true";
  //     String expectedUrl =
  //         "jdbc:postgresql://localhost:5432/postgres?user=admin&password=p@ssw0rd!&ssl=true";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }

  //   @Test
  //   @DisplayName("Should throw exception for null URL")
  //   public void testNullUrl() {
  //     IllegalArgumentException exception =
  //         assertThrows(
  //             IllegalArgumentException.class, () ->
  // SystemDatabase.createPostgresConnectionUrl(null));

  //     assertTrue(exception.getMessage().contains("Invalid PostgreSQL JDBC URL"));
  //   }

  //   @Test
  //   @DisplayName("Should throw exception for non-PostgreSQL JDBC URL")
  //   public void testNonPostgreSqlUrl() {
  //     String invalidUrl = "jdbc:mysql://localhost:3306/mydb";

  //     IllegalArgumentException exception =
  //         assertThrows(
  //             IllegalArgumentException.class,
  //             () -> SystemDatabase.createPostgresConnectionUrl(invalidUrl));

  //     assertTrue(exception.getMessage().contains("Invalid PostgreSQL JDBC URL"));
  //   }

  //   @Test
  //   @DisplayName("Should throw exception for malformed JDBC URL")
  //   public void testMalformedUrl() {
  //     String malformedUrl = "jdbc:postgresql://localhost:5432";

  //     IllegalArgumentException exception =
  //         assertThrows(
  //             IllegalArgumentException.class,
  //             () -> SystemDatabase.createPostgresConnectionUrl(malformedUrl));

  //     assertTrue(exception.getMessage().contains("missing database name"));
  //   }

  //   @Test
  //   @DisplayName("Should throw exception for empty string URL")
  //   public void testEmptyStringUrl() {
  //     IllegalArgumentException exception =
  //         assertThrows(
  //             IllegalArgumentException.class, () ->
  // SystemDatabase.createPostgresConnectionUrl(""));

  //     assertTrue(exception.getMessage().contains("Invalid PostgreSQL JDBC URL"));
  //   }

  //   @Test
  //   @DisplayName("Should preserve IPv6 addresses")
  //   public void testIpv6Address() {
  //     String originalUrl = "jdbc:postgresql://[::1]:5432/testdb?user=admin";
  //     String expectedUrl = "jdbc:postgresql://[::1]:5432/postgres?user=admin";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }

  //   @Test
  //   @DisplayName("Should handle URL with fragment-like characters in parameters")
  //   public void testUrlWithFragmentCharacters() {
  //     String originalUrl =
  //         "jdbc:postgresql://localhost:5432/mydb?user=admin&options=-c%20search_path=schema1";
  //     String expectedUrl =
  //
  // "jdbc:postgresql://localhost:5432/postgres?user=admin&options=-c%20search_path=schema1";

  //     String result = SystemDatabase.createPostgresConnectionUrl(originalUrl);

  //     assertEquals(expectedUrl, result);
  //   }
}
