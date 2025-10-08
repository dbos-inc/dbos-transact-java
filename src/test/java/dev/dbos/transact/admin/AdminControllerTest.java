package dev.dbos.transact.admin;
// package dev.dbos.transact.http.controllers;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ServerSocket;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

// import dev.dbos.transact.DBOS;
// import dev.dbos.transact.config.DBOSConfig;
// import dev.dbos.transact.context.WorkflowOptions;
// import dev.dbos.transact.execution.ExecutingService;
// import dev.dbos.transact.execution.ExecutingServiceImpl;
// import dev.dbos.transact.utils.DBUtils;
// import dev.dbos.transact.workflow.ForkService;
// import dev.dbos.transact.workflow.ForkServiceImpl;
// import dev.dbos.transact.workflow.SimpleService;
// import dev.dbos.transact.workflow.SimpleServiceImpl;
// import dev.dbos.transact.workflow.WorkflowState;

// import java.net.URI;
// import java.net.http.HttpClient;
// import java.net.http.HttpRequest;
// import java.net.http.HttpResponse;
// import java.sql.Connection;
// import java.sql.PreparedStatement;
// import java.sql.SQLException;
// import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junitpioneer.jupiter.RetryingTest;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.WorkflowHandle;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
class AdminControllerTest {

  int port;
  SystemDatabase mockDB;
  DBOSExecutor mockExec;

  @BeforeEach
  void beforeEach() throws IOException {

    try (var socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    mockDB = mock(SystemDatabase.class);
    mockExec = mock(DBOSExecutor.class);
  }

  @Test
  public void healthz() throws IOException {
    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .when()
          .get("/dbos-healthz")
          .then()
          .statusCode(200)
          .body(equalTo("healthy"));
    }
  }

  @Test
  public void perf() throws IOException {
    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .when()
          .get("/dbos-perf")
          .then()
          .statusCode(500)
          .body(equalTo("not implemented"));
    }
  }

  @Test
  public void deactivate() throws IOException {
    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .when()
          .get("/dbos-deactivate")
          .then()
          .statusCode(500)
          .body(equalTo("not implemented"));
    }
  }

  @Test
  public void workflowRecovery() throws IOException {

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      var handle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
      when(handle.getWorkflowId()).thenReturn("workflow-00%d".formatted(i));
      handles.add(handle);
    }

    List<String> param = List.of("local");
    when(mockExec.recoverPendingWorkflows(eq(param))).thenReturn(handles);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body("[\"local\"]")
          .when()
          .post("/dbos-workflow-recovery")
          .then()
          .statusCode(200)
          .body("size()", equalTo(5))
          .body("[0]", equalTo("workflow-000"))
          .body("[1]", equalTo("workflow-001"))
          .body("[2]", equalTo("workflow-002"))
          .body("[3]", equalTo("workflow-003"))
          .body("[4]", equalTo("workflow-004"));
    }
  }

  @Test
  public void ensurePostJsonNotPost() throws IOException {

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      var handle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
      when(handle.getWorkflowId()).thenReturn("workflow-00%d".formatted(i));
      handles.add(handle);
    }

    List<String> param = List.of("local");
    when(mockExec.recoverPendingWorkflows(eq(param))).thenReturn(handles);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .when()
          .get("/dbos-workflow-recovery")
          .then()
          .statusCode(405);
    }
  }

  @Test
  public void ensurePostJsonNotJson() throws IOException {

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      var handle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
      when(handle.getWorkflowId()).thenReturn("workflow-00%d".formatted(i));
      handles.add(handle);
    }

    List<String> param = List.of("local");
    when(mockExec.recoverPendingWorkflows(eq(param))).thenReturn(handles);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .body("[\"local\"]")
          .when()
          .post("/dbos-workflow-recovery")
          .then()
          .statusCode(415);
    }
  }



  // @Test
  // public void recovery() throws Exception {
  // ExecutingService executingService =
  // dbos.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
  // SimpleService simpleService =
  // dbos.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

  // dbos.launch();

  // // Needed to call the step
  // executingService.setExecutingService(executingService);

  // // Execute multiple workflows with different IDs and inputs
  // try (var id1 = new WorkflowOptions("workflow-001").setContext()) {
  // executingService.workflowMethodWithStep("input-alpha");
  // }

  // try (var id2 = new WorkflowOptions("workflow-002").setContext()) {
  // executingService.workflowMethodWithStep("input-beta");
  // }

  // try (var id3 = new WorkflowOptions("workflow-003").setContext()) {
  // executingService.workflowMethodWithStep("input-gamma");
  // }

  // try (var id4 = new WorkflowOptions("workflow-004").setContext()) {
  // simpleService.workWithString("input-delta");
  // }

  // String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? ;";
  // try (Connection conn = DBUtils.getConnection(dbosConfig);
  // PreparedStatement pstmt = conn.prepareStatement(sql)) {

  // pstmt.setString(1, WorkflowState.PENDING.name());
  // pstmt.setLong(2, Instant.now().toEpochMilli());
  // pstmt.executeUpdate();
  // }

  // given()
  // .port(3010)
  // .contentType("application/json")
  // .body("[\"local\"]")
  // .when()
  // .post("/dbos-workflow-recovery")
  // .then()
  // .statusCode(200)
  // .body("size()", equalTo(4))
  // .body("[0]", equalTo("workflow-001"))
  // .body("[1]", equalTo("workflow-002"))
  // .body("[2]", equalTo("workflow-003"))
  // .body("[3]", equalTo("workflow-004"));
  // }

  // @RetryingTest(3)
  // public void queueMetadata() throws Exception {
  // dbos.Queue("firstQueue").concurrency(1).workerConcurrency(1).build();

  // dbos.Queue("secondQueue").limit(2, 4.5).priorityEnabled(true).build();

  // dbos.launch();

  // given()
  // .port(3010)
  // .when()
  // .get("/dbos-workflow-queues-metadata")
  // .then()
  // .statusCode(200)
  // .body("size()", equalTo(4))
  // .body("find { it.name == 'firstQueue' }.concurrency", equalTo(1))
  // .body("find { it.name == 'firstQueue' }.workerConcurrency", equalTo(1))
  // .body("find { it.name == 'firstQueue' }.rateLimit", nullValue())
  // .body("find { it.name == 'firstQueue' }.priorityEnabled", equalTo(false))
  // .body("find { it.name == 'secondQueue' }.concurrency", equalTo(0))
  // .body("find { it.name == 'secondQueue' }.workerConcurrency", equalTo(0))
  // .body("find { it.name == 'secondQueue' }.rateLimit.limit", equalTo(2))
  // .body("find { it.name == 'secondQueue' }.rateLimit.period", equalTo(4.5f))
  // .body("find { it.name == 'secondQueue' }.priorityEnabled", equalTo(true));
  // }

  // @Test
  // public void listWorkflowSteps() throws Exception {
  // ExecutingService executingService =
  // dbos.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
  // dbos.launch();

  // // Needed to call the step
  // executingService.setExecutingService(executingService);

  // try (var id = new WorkflowOptions("abc123").setContext()) {
  // String result = executingService.workflowMethodWithStep("test-item");
  // assertEquals("test-itemstepOnestepTwo", result);
  // }

  // given()
  // .port(3010)
  // .when()
  // .get("/workflows/abc123/steps")
  // .then()
  // .statusCode(200)
  // .body("size()", equalTo(2))
  // .body("[0].functionId", equalTo(0))
  // .body("[0].functionName", equalTo("stepOne"))
  // .body("[0].output", equalTo("stepOne"))
  // .body("[0].error", nullValue())
  // .body("[0].childWorkflowId", nullValue())
  // .body("[1].functionId", equalTo(1))
  // .body("[1].functionName", equalTo("stepTwo"))
  // .body("[1].output", equalTo("stepTwo"))
  // .body("[1].error", nullValue())
  // .body("[1].childWorkflowId", nullValue());
  // }

  // @Test
  // public void getWorkflowStatus() throws Exception {
  // ExecutingService executingService =
  // dbos.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
  // dbos.launch();

  // // Needed to call the step
  // executingService.setExecutingService(executingService);

  // try (var id = new WorkflowOptions("abc123").setContext()) {
  // String result = executingService.workflowMethodWithStep("test-item");
  // assertEquals("test-itemstepOnestepTwo", result);
  // }

  // given()
  // .port(3010)
  // .when()
  // .get("/workflows/abc123")
  // .then()
  // .statusCode(200)
  // .body("workflowId", equalTo("abc123"))
  // .body("status", equalTo("SUCCESS"))
  // .body("name", equalTo("workflowMethodWithStep"))
  // .body("className",
  // equalTo("dev.dbos.transact.execution.ExecutingServiceImpl"))
  // .body("input", hasSize(1))
  // .body("input[0]", equalTo("test-item"))
  // .body("output", equalTo("test-itemstepOnestepTwo"))
  // .body("error", nullValue());
  // }

  // @Test
  // public void workflows() throws Exception {
  // ExecutingService executingService =
  // dbos.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
  // SimpleService simpleService =
  // dbos.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
  // dbos.launch();

  // // Needed to call the step
  // executingService.setExecutingService(executingService);

  // // Execute multiple workflows with different IDs and inputs
  // try (var id1 = new WorkflowOptions("workflow-001").setContext()) {
  // String result1 = executingService.workflowMethodWithStep("input-alpha");
  // assertEquals("input-alphastepOnestepTwo", result1);
  // }

  // try (var id2 = new WorkflowOptions("workflow-002").setContext()) {
  // String result2 = executingService.workflowMethodWithStep("input-beta");
  // assertEquals("input-betastepOnestepTwo", result2);
  // }

  // try (var id3 = new WorkflowOptions("workflow-003").setContext()) {
  // String result3 = executingService.workflowMethodWithStep("input-gamma");
  // assertEquals("input-gammastepOnestepTwo", result3);
  // }

  // try (var id4 = new WorkflowOptions("workflow-004").setContext()) {
  // String result3 = simpleService.workWithString("input-delta");
  // assertEquals("Processed: input-delta", result3);
  // }

  // var e =
  // assertThrows(
  // Exception.class,
  // () -> {
  // try (var id5 = new WorkflowOptions("workflow-005").setContext()) {
  // simpleService.workWithError();
  // }
  // });
  // assertEquals("DBOS Test error", e.getMessage());

  // given()
  // .port(3010)
  // .contentType("application/json")
  // .when()
  // .post("/workflows")
  // .then()
  // .statusCode(200)
  // .body("size()", equalTo(5));

  // given()
  // .port(3010)
  // .contentType("application/json")
  // .body("{ }")
  // .when()
  // .post("/workflows")
  // .then()
  // .statusCode(200)
  // .body("size()", equalTo(5));

  // given()
  // .port(3010)
  // .contentType("application/json")
  // .body("{ \"status\": \"SUCCESS\" }")
  // .when()
  // .post("/workflows")
  // .then()
  // .statusCode(200)
  // .body("size()", equalTo(4));

  // given()
  // .port(3010)
  // .contentType("application/json")
  // .body("{ \"status\": \"ERROR\" }")
  // .when()
  // .post("/workflows")
  // .then()
  // .statusCode(200)
  // .body("size()", equalTo(1));

  // given()
  // .port(3010)
  // .contentType("application/json")
  // .body("{ \"workflowName\": \"workflowMethodWithStep\" }")
  // .when()
  // .post("/workflows")
  // .then()
  // .statusCode(200)
  // .body("size()", equalTo(3));
  // }

  // @Test
  // public void fork() throws Exception {

  // ForkServiceImpl impl = new ForkServiceImpl();
  // ForkService forkService = dbos.registerWorkflows(ForkService.class, impl);
  // forkService.setForkService(forkService);
  // dbos.launch();

  // String workflowId = "wfid1";
  // try (var id = new WorkflowOptions(workflowId).setContext()) {
  // String result = forkService.simpleWorkflow("hello");
  // assertEquals("hellohello", result);
  // }

  // var handle = dbos.retrieveWorkflow(workflowId);
  // assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());

  // assertEquals(1, impl.step1Count);
  // assertEquals(1, impl.step2Count);
  // assertEquals(1, impl.step3Count);
  // assertEquals(1, impl.step4Count);
  // assertEquals(1, impl.step5Count);

  // String newWorkflowId =
  // given()
  // .port(3010)
  // .contentType("application/json")
  // .body("{ \"startStep\": 3 }")
  // .when()
  // .post("/workflows/" + workflowId + "/fork")
  // .then()
  // .statusCode(200)
  // .body("workflowId", notNullValue())
  // .extract()
  // .path("workflowId");

  // var newHandle = dbos.retrieveWorkflow(newWorkflowId);
  // assertEquals("hellohello", newHandle.getResult());

  // assertEquals(1, impl.step1Count);
  // assertEquals(1, impl.step2Count);
  // assertEquals(1, impl.step3Count);
  // assertEquals(2, impl.step4Count);
  // assertEquals(2, impl.step5Count);
  // }
}
