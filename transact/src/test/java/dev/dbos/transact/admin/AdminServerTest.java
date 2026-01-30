package dev.dbos.transact.admin;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.utils.WorkflowStatusBuilder;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class AdminServerTest {

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

  @SuppressWarnings("unchecked")
  @Test
  public void ensurePostJsonNotPost() throws IOException {

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      var handle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
      when(handle.workflowId()).thenReturn("workflow-00%d".formatted(i));
      handles.add(handle);
    }

    List<String> param = List.of("local");
    when(mockExec.recoverPendingWorkflows(eq(param))).thenReturn(handles);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given().port(port).when().get("/dbos-workflow-recovery").then().statusCode(405);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void ensurePostJsonNotJson() throws IOException {

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      var handle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
      when(handle.workflowId()).thenReturn("workflow-00%d".formatted(i));
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

  @Test
  public void exceptionCatching500() throws IOException {
    var exception = new RuntimeException("test-exception");
    when(mockExec.getQueues()).thenThrow(exception);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body("[\"local\"]")
          .when()
          .post("/dbos-workflow-queues-metadata")
          .then()
          .statusCode(500)
          .body(equalTo(exception.getMessage()));
    }
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
          .body("status", equalTo("healthy"));
    }
  }

  @Test
  public void deactivate() throws IOException {
    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .when()
          .get("/deactivate")
          .then()
          .statusCode(200)
          .body(equalTo("deactivated"));

      verify(mockExec).deactivateLifecycleListeners();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void workflowRecovery() throws IOException {

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      var handle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
      when(handle.workflowId()).thenReturn("workflow-00%d".formatted(i));
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
  public void queueMetadata() throws IOException {
    var queue1 = new Queue("test-queue-1");
    var queue2 =
        new Queue("test-queue-2")
            .withConcurrency(10)
            .withWorkerConcurrency(5)
            .withPriorityEnabled(true)
            .withRateLimit(2, 4.0);

    when(mockExec.getQueues()).thenReturn(List.of(queue1, queue2));

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body("[\"local\"]")
          .when()
          .post("/dbos-workflow-queues-metadata")
          .then()
          .statusCode(200)
          .body("size()", equalTo(2))
          .body("[0].name", equalTo("test-queue-1"))
          .body("[0].concurrency", nullValue())
          .body("[0].workerConcurrency", nullValue())
          .body("[0].priorityEnabled", equalTo(false))
          .body("[0].rateLimit", nullValue())
          .body("[1].name", equalTo("test-queue-2"))
          .body("[1].concurrency", equalTo(10))
          .body("[1].workerConcurrency", equalTo(5))
          .body("[1].priorityEnabled", equalTo(true))
          .body("[1].rateLimit", notNullValue())
          .body("[1].rateLimit.limit", equalTo(2))
          .body("[1].rateLimit.period", equalTo(4.0f));
    }
  }

  @Test
  public void garbageCollect() throws IOException {

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body(
              """
              { "cutoff_epoch_timestamp_ms": 42, "rows_threshold": 37 } """)
          .when()
          .post("/dbos-garbage-collect")
          .then()
          .statusCode(204);

      verify(mockDB).garbageCollect(eq(42L), eq(37L));
    }
  }

  @Test
  public void globalTimeout() throws IOException {

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body(
              """
              { "cutoff_epoch_timestamp_ms": 42 } """)
          .when()
          .post("/dbos-global-timeout")
          .then()
          .statusCode(204);

      verify(mockExec).globalTimeout(eq(42L));
    }
  }

  @Test
  public void listWorkflows() throws IOException {

    List<WorkflowStatus> statuses = new ArrayList<WorkflowStatus>();
    statuses.add(
        new WorkflowStatusBuilder("wf-1")
            .status(WorkflowState.PENDING)
            .name("WF1")
            .createdAt(1754936102215L)
            .updatedAt(1754936102215L)
            .executorId("test-executor")
            .appVersion("test-app-ver")
            .appId("test-app-id")
            .build());
    statuses.add(
        new WorkflowStatusBuilder("wf-2")
            .status(WorkflowState.PENDING)
            .name("WF2")
            .createdAt(1754936722066L)
            .updatedAt(1754936722066L)
            .executorId("test-executor")
            .appVersion("test-app-ver")
            .appId("test-app-id")
            .build());
    statuses.add(
        new WorkflowStatusBuilder("wf-3")
            .status(WorkflowState.PENDING)
            .name("WF3")
            .createdAt(1754946202215L)
            .updatedAt(1754946202215L)
            .executorId("test-executor")
            .appVersion("test-app-ver")
            .appId("test-app-id")
            .build());

    when(mockDB.listWorkflows(any())).thenReturn(statuses);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body(
              """
                      {
                  "workflow_id_prefix": "WF",
                  "end_time": "2025-10-09T11:26:05-07:00"
                    } """)
          .when()
          .post("/workflows")
          .then()
          .statusCode(200)
          .body("size()", equalTo(3))
          .body("[0].WorkflowUUID", equalTo("wf-1"))
          .body("[0].Status", equalTo("PENDING"))
          .body("[0].CreatedAt", equalTo("1754936102215"));

      ArgumentCaptor<ListWorkflowsInput> inputCaptor =
          ArgumentCaptor.forClass(ListWorkflowsInput.class);

      verify(mockDB).listWorkflows(inputCaptor.capture());
      var input = inputCaptor.getValue();
      assertEquals("WF", input.workflowIdPrefix());
      assertEquals(OffsetDateTime.parse("2025-10-09T11:26:05-07:00"), input.endTime());
    }
  }

  @Test
  public void listQueuedWorkflows() throws IOException {

    List<WorkflowStatus> statuses = new ArrayList<WorkflowStatus>();
    statuses.add(
        new WorkflowStatusBuilder("wf-1")
            .status(WorkflowState.PENDING)
            .name("WF1")
            .createdAt(1754936102215L)
            .updatedAt(1754936102215L)
            .executorId("test-executor")
            .appVersion("test-app-ver")
            .appId("test-app-id")
            .queueName("test-queue-name")
            .build());
    statuses.add(
        new WorkflowStatusBuilder("wf-2")
            .status(WorkflowState.PENDING)
            .name("WF2")
            .createdAt(1754936722066L)
            .updatedAt(1754936722066L)
            .executorId("test-executor")
            .appVersion("test-app-ver")
            .appId("test-app-id")
            .queueName("test-queue-name")
            .build());
    statuses.add(
        new WorkflowStatusBuilder("wf-3")
            .status(WorkflowState.PENDING)
            .name("WF3")
            .createdAt(1754946202215L)
            .updatedAt(1754946202215L)
            .executorId("test-executor")
            .appVersion("test-app-ver")
            .appId("test-app-id")
            .queueName("test-queue-name")
            .build());

    when(mockDB.listWorkflows(any())).thenReturn(statuses);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body(
              """
                      {
                  "queue_name": "some-queue",
                  "end_time": "2025-10-09T11:26:05-07:00"
                    } """)
          .when()
          .post("/queues")
          .then()
          .statusCode(200)
          .body("size()", equalTo(3))
          .body("[0].WorkflowUUID", equalTo("wf-1"))
          .body("[0].Status", equalTo("PENDING"))
          .body("[0].CreatedAt", equalTo("1754936102215"))
          .body("[0].QueueName", equalTo("test-queue-name"));

      ArgumentCaptor<ListWorkflowsInput> inputCaptor =
          ArgumentCaptor.forClass(ListWorkflowsInput.class);

      verify(mockDB).listWorkflows(inputCaptor.capture());
      var input = inputCaptor.getValue();
      assertEquals("some-queue", input.queueName());
      assertTrue(input.queuesOnly());
      assertEquals(OffsetDateTime.parse("2025-10-09T11:26:05-07:00"), input.endTime());
    }
  }

  @Test
  public void getWorkflow() throws IOException {
    var status =
        new WorkflowStatusBuilder("test-wf-id")
            .status(WorkflowState.PENDING)
            .name("WF3")
            .createdAt(1754946202215L)
            .updatedAt(1754946202215L)
            .build();

    when(mockDB.listWorkflows(any())).thenReturn(List.of(status));

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .when()
          .get("/workflows/test-wf-id")
          .then()
          .statusCode(200)
          .body("WorkflowUUID", equalTo(status.workflowId()))
          .body("WorkflowName", equalTo(status.name()))
          .body("Status", equalTo(status.status().toString()))
          .body("CreatedAt", equalTo("1754946202215"));

      ArgumentCaptor<ListWorkflowsInput> inputCaptor =
          ArgumentCaptor.forClass(ListWorkflowsInput.class);

      verify(mockDB).listWorkflows(inputCaptor.capture());
      var input = inputCaptor.getValue();
      assertEquals(List.of(status.workflowId()), input.workflowIds());

      assertNull(input.authenticatedUser());
      assertNull(input.queueName());
      assertNull(input.startTime());
      assertNull(input.endTime());
      assertNull(input.workflowIdPrefix());
      assertNull(input.applicationVersion());
      assertNull(input.offset());
      assertNull(input.limit());
      assertNull(input.status());
      assertNull(input.executorIds());
    }
  }

  @Test
  public void getWorkflowNotFound() throws IOException {
    List<WorkflowStatus> statuses = new ArrayList<WorkflowStatus>();
    when(mockDB.listWorkflows(any())).thenReturn(statuses);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .when()
          .get("/workflows/test-wf-id")
          .then()
          .statusCode(404)
          .body(equalTo("Workflow not found"));
    }
  }

  @Test
  public void listSteps() throws IOException {

    List<StepInfo> steps = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      var step =
          new StepInfo(i, "step-%d".formatted(i), "output-%d".formatted(i), null, null, null, null);
      steps.add(step);
    }
    steps.add(new StepInfo(3, "step-3", null, null, "child-wfid-3", null, null));
    var error = new RuntimeException("error-4");
    steps.add(new StepInfo(4, "step-4", null, ErrorResult.fromThrowable(error), null, null, null));

    when(mockDB.listWorkflowSteps(any())).thenReturn(steps);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      Response response =
          given()
              .port(port)
              .when()
              .get("/workflows/test-wf-id/steps")
              .then()
              .statusCode(200)
              .extract()
              .response();

      List<Map<String, Object>> stepsReturn = response.jsonPath().getList("");
      for (var i = 0; i < stepsReturn.size(); i++) {
        var step = stepsReturn.get(i);
        assertEquals(i, step.get("function_id"));
        assertEquals("step-%d".formatted(i), step.get("function_name"));
        if (i < 3) {
          assertEquals("\"output-%d\"".formatted(i), step.get("output"));
          assertNull(step.get("error"));
          assertNull(step.get("child_workflow_id"));
        }
        if (i == 3) {
          assertNull(step.get("output"));
          assertNull(step.get("error"));
          assertEquals("child-wfid-3", step.get("child_workflow_id"));
        }
        if (i == 4) {
          assertNull(step.get("output"));
          assertNotNull(step.get("error"));
          var result = JSONUtil.fromJson((String) step.get("error"), ErrorResult.class);
          assertEquals("error-4", result.message());
          assertNull(step.get("child_workflow_id"));
        }
      }
    }
  }

  @Test
  public void cancelWorkflow() throws IOException {

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given().port(port).when().post("/workflows/test-wf-id/cancel").then().statusCode(204);

      verify(mockExec).cancelWorkflow(eq("test-wf-id"));
    }
  }

  @Test
  public void resumeWorkflow() throws IOException {

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given().port(port).when().post("/workflows/test-wf-id/resume").then().statusCode(204);

      verify(mockExec).resumeWorkflow(eq("test-wf-id"));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void forkWorkflowNoOptions() throws IOException {
    var newWfId = "test-new-wf-id";
    var mockHandle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
    when(mockHandle.workflowId()).thenReturn(newWfId);
    when(mockExec.forkWorkflow(eq("test-wf-id"), eq(0), any())).thenReturn(mockHandle);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body("{}")
          .when()
          .post("/workflows/test-wf-id/fork")
          .then()
          .statusCode(200)
          .body("workflow_id", equalTo(newWfId));

      ArgumentCaptor<ForkOptions> optionsCaptor = ArgumentCaptor.forClass(ForkOptions.class);

      verify(mockExec).forkWorkflow(eq("test-wf-id"), eq(0), optionsCaptor.capture());
      var options = optionsCaptor.getValue();
      assertNull(options.applicationVersion());
      assertNull(options.forkedWorkflowId());
      assertNull(options.timeout());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void forkWorkflowWithOptions() throws IOException {
    var newWfId = "test-new-wf-id";

    var mockHandle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
    when(mockHandle.workflowId()).thenReturn(newWfId);

    when(mockExec.forkWorkflow(eq("test-wf-id"), eq(2), any())).thenReturn(mockHandle);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given()
          .port(port)
          .contentType("application/json")
          .body(
              "{\"start_step\": 2, \"new_workflow_id\": \"test-new-wf-id\", \"application_version\": \"test-application_version\"}")
          .when()
          .post("/workflows/test-wf-id/fork")
          .then()
          .statusCode(200)
          .body("workflow_id", equalTo(newWfId));

      ArgumentCaptor<ForkOptions> optionsCaptor = ArgumentCaptor.forClass(ForkOptions.class);

      verify(mockExec).forkWorkflow(eq("test-wf-id"), eq(2), optionsCaptor.capture());
      var options = optionsCaptor.getValue();
      assertEquals("test-application_version", options.applicationVersion());
      assertEquals("test-new-wf-id", options.forkedWorkflowId());
      assertNull(options.timeout());
    }
  }
}
