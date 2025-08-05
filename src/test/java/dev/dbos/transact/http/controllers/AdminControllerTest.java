package dev.dbos.transact.http.controllers;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

import static org.junit.jupiter.api.Assertions.*;

class AdminControllerTest {

    private static DBOSConfig dbosConfig;
    private DBOS dbos;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        AdminControllerTest.dbosConfig = new DBOSConfig.Builder()
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

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:3010/dbos-healthz"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals("healthy", response.body());
    }

    @Test
    public void queueMetadata() throws Exception {
        new DBOS.QueueBuilder("firstQueue")
                .concurrency(1)
                .workerConcurrency(1)
                .build();

        new DBOS.QueueBuilder("secondQueue")
                .limit(2, 4.5)
                .priorityEnabled(true)
                .build();

        given()
                .port(3010)
                .when()
                .get("/dbos-workflow-queues-metadata")
                .then()
                .statusCode(200)
                .body("size()", equalTo(4))
                .body("find { it.name == 'firstQueue' }.concurrency", equalTo(1))
                .body("find { it.name == 'firstQueue' }.workerConcurrency", equalTo(1))
                .body("find { it.name == 'firstQueue' }.rateLimit", nullValue())
                .body("find { it.name == 'firstQueue' }.priorityEnabled", equalTo(false))
                .body("find { it.name == 'secondQueue' }.concurrency", equalTo(0))
                .body("find { it.name == 'secondQueue' }.workerConcurrency", equalTo(0))
                .body("find { it.name == 'secondQueue' }.rateLimit.limit", equalTo(2))
                .body("find { it.name == 'secondQueue' }.rateLimit.period", equalTo(4.5f))
                .body("find { it.name == 'secondQueue' }.priorityEnabled", equalTo(true));

    }

}