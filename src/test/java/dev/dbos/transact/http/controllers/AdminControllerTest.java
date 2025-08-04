package dev.dbos.transact.http.controllers;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdminControllerTest {

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    AdminControllerTest.dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .dbHost("localhost")
            .dbPort(5432)
            .dbUser("postgres")
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .runAdminServer()
            .adminServerPort(3010)
            .adminAwaitOnStart(false)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dbos = DBOS.initialize(dbosConfig);
    dbos.launch();
  }

  @AfterEach
  void afterEachTest() throws SQLException {
    dbos.shutdown();
  }

  @Test
  public void health() throws Exception {

    HttpClient client = HttpClient.newHttpClient();

    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create("http://localhost:3010/healthz")).GET().build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode());
    assertEquals("Healthy", response.body());
  }
}
