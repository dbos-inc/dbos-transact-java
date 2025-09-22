package dev.dbos.transact.http.controllers;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.execution.ExecutingService;
import dev.dbos.transact.execution.ExecutingServiceImpl;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ForkService;
import dev.dbos.transact.workflow.ForkServiceImpl;
import dev.dbos.transact.workflow.SimpleService;
import dev.dbos.transact.workflow.SimpleServiceImpl;
import dev.dbos.transact.workflow.WorkflowState;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

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
            .adminAwaitOnStart(true)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws SQLException, Exception {
    dbos.shutdown();
  }

  @Test
  public void health() throws Exception {

    dbos.launch();
    HttpClient client = HttpClient.newHttpClient();

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:3010/dbos-healthz"))
            .GET()
            .build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode());
    assertEquals("healthy", response.body());
  }

  @Test
  public void recovery() throws Exception {
    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    // Needed to call the step
    executingService.setExecutingService(executingService);

    // Execute multiple workflows with different IDs and inputs
    try (SetWorkflowID id1 = new SetWorkflowID("workflow-001")) {
      executingService.workflowMethodWithStep("input-alpha");
    }

    try (SetWorkflowID id2 = new SetWorkflowID("workflow-002")) {
      executingService.workflowMethodWithStep("input-beta");
    }

    try (SetWorkflowID id3 = new SetWorkflowID("workflow-003")) {
      executingService.workflowMethodWithStep("input-gamma");
    }

    try (SetWorkflowID id4 = new SetWorkflowID("workflow-004")) {
      simpleService.workWithString("input-delta");
    }

    String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? ;";
    try (Connection conn = DBUtils.getConnection(dbosConfig);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {

      pstmt.setString(1, WorkflowState.PENDING.name());
      pstmt.setLong(2, Instant.now().toEpochMilli());
      pstmt.executeUpdate();
    }

    given()
        .port(3010)
        .contentType("application/json")
        .body("[\"local\"]")
        .when()
        .post("/dbos-workflow-recovery")
        .then()
        .statusCode(200)
        .body("size()", equalTo(4))
        .body("[0]", equalTo("workflow-001"))
        .body("[1]", equalTo("workflow-002"))
        .body("[2]", equalTo("workflow-003"))
        .body("[3]", equalTo("workflow-004"));
  }

  @Test
  public void queueMetadata() throws Exception {
    dbos.Queue("firstQueue").concurrency(1).workerConcurrency(1).build();

    dbos.Queue("secondQueue").limit(2, 4.5).priorityEnabled(true).build();

    dbos.launch();

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

  @Test
  public void listWorkflowSteps() throws Exception {
    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();
    dbos.launch();

    // Needed to call the step
    executingService.setExecutingService(executingService);

    try (SetWorkflowID id = new SetWorkflowID("abc123")) {
      String result = executingService.workflowMethodWithStep("test-item");
      assertEquals("test-itemstepOnestepTwo", result);
    }

    given()
        .port(3010)
        .when()
        .get("/workflows/abc123/steps")
        .then()
        .statusCode(200)
        .body("size()", equalTo(2))
        .body("[0].functionId", equalTo(0))
        .body("[0].functionName", equalTo("stepOne"))
        .body("[0].output", equalTo("stepOne"))
        .body("[0].error", nullValue())
        .body("[0].childWorkflowId", nullValue())
        .body("[1].functionId", equalTo(1))
        .body("[1].functionName", equalTo("stepTwo"))
        .body("[1].output", equalTo("stepTwo"))
        .body("[1].error", nullValue())
        .body("[1].childWorkflowId", nullValue());
  }

  @Test
  public void getWorkflowStatus() throws Exception {
    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();
    dbos.launch();

    // Needed to call the step
    executingService.setExecutingService(executingService);

    try (SetWorkflowID id = new SetWorkflowID("abc123")) {
      String result = executingService.workflowMethodWithStep("test-item");
      assertEquals("test-itemstepOnestepTwo", result);
    }

    given()
        .port(3010)
        .when()
        .get("/workflows/abc123")
        .then()
        .statusCode(200)
        .body("workflowId", equalTo("abc123"))
        .body("status", equalTo("SUCCESS"))
        .body("name", equalTo("workflowMethodWithStep"))
        .body("className", equalTo("dev.dbos.transact.execution.ExecutingServiceImpl"))
        .body("input", hasSize(1))
        .body("input[0]", equalTo("test-item"))
        .body("output", equalTo("test-itemstepOnestepTwo"))
        .body("error", nullValue());
  }

  @Test
  public void workflows() throws Exception {
    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    dbos.launch();

    // Needed to call the step
    executingService.setExecutingService(executingService);

    // Execute multiple workflows with different IDs and inputs
    try (SetWorkflowID id1 = new SetWorkflowID("workflow-001")) {
      String result1 = executingService.workflowMethodWithStep("input-alpha");
      assertEquals("input-alphastepOnestepTwo", result1);
    }

    try (SetWorkflowID id2 = new SetWorkflowID("workflow-002")) {
      String result2 = executingService.workflowMethodWithStep("input-beta");
      assertEquals("input-betastepOnestepTwo", result2);
    }

    try (SetWorkflowID id3 = new SetWorkflowID("workflow-003")) {
      String result3 = executingService.workflowMethodWithStep("input-gamma");
      assertEquals("input-gammastepOnestepTwo", result3);
    }

    try (SetWorkflowID id4 = new SetWorkflowID("workflow-004")) {
      String result3 = simpleService.workWithString("input-delta");
      assertEquals("Processed: input-delta", result3);
    }

    try (SetWorkflowID id5 = new SetWorkflowID("workflow-005")) {
      simpleService.workWithError();
    } catch (Exception e) {
      assertEquals("DBOS Test error", e.getMessage());
    }

    given()
        .port(3010)
        .contentType("application/json")
        .when()
        .post("/workflows")
        .then()
        .statusCode(200)
        .body("size()", equalTo(5));

    given()
        .port(3010)
        .contentType("application/json")
        .body("{ }")
        .when()
        .post("/workflows")
        .then()
        .statusCode(200)
        .body("size()", equalTo(5));

    given()
        .port(3010)
        .contentType("application/json")
        .body("{ \"status\": \"SUCCESS\" }")
        .when()
        .post("/workflows")
        .then()
        .statusCode(200)
        .body("size()", equalTo(4));

    given()
        .port(3010)
        .contentType("application/json")
        .body("{ \"status\": \"ERROR\" }")
        .when()
        .post("/workflows")
        .then()
        .statusCode(200)
        .body("size()", equalTo(1));

    given()
        .port(3010)
        .contentType("application/json")
        .body("{ \"workflowName\": \"workflowMethodWithStep\" }")
        .when()
        .post("/workflows")
        .then()
        .statusCode(200)
        .body("size()", equalTo(3));
  }

  @Test
  public void fork() throws Exception {

    ForkServiceImpl impl = new ForkServiceImpl();

    ForkService forkService =
        dbos.<ForkService>Workflow().interfaceClass(ForkService.class).implementation(impl).build();
    forkService.setForkService(forkService);
    dbos.launch();

    String workflowId = "wfid1";
    try (SetWorkflowID id = new SetWorkflowID(workflowId)) {
      String result = forkService.simpleWorkflow("hello");
      assertEquals("hellohello", result);
    }

    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);
    assertEquals(1, impl.step4Count);
    assertEquals(1, impl.step5Count);

    String newWorkflowId =
        given()
            .port(3010)
            .contentType("application/json")
            .body("{ \"startStep\": 3 }")
            .when()
            .post("/workflows/" + workflowId + "/fork")
            .then()
            .statusCode(200)
            .body("workflowId", notNullValue())
            .extract()
            .path("workflowId");

    var newHandle = dbos.retrieveWorkflow(newWorkflowId);
    assertEquals("hellohello", newHandle.getResult());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);
    assertEquals(2, impl.step4Count);
    assertEquals(2, impl.step5Count);
  }
}
