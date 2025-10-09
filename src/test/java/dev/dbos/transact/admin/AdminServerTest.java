package dev.dbos.transact.admin;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.WorkflowStatusBuilder;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
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

  @Test
  public void ensurePostJsonNotPost() throws IOException {

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      @SuppressWarnings("unchecked")
      var handle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
      when(handle.getWorkflowId()).thenReturn("workflow-00%d".formatted(i));
      handles.add(handle);
    }

    List<String> param = List.of("local");
    when(mockExec.recoverPendingWorkflows(eq(param))).thenReturn(handles);

    try (var server = new AdminServer(port, mockExec, mockDB)) {
      server.start();

      given().port(port).when().get("/dbos-workflow-recovery").then().statusCode(405);
    }
  }

  @Test
  public void ensurePostJsonNotJson() throws IOException {

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      @SuppressWarnings("unchecked")
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
      @SuppressWarnings("unchecked")
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
  public void queueMetadata() throws IOException {
    var queue1 = new Queue("test-queue-1", 0, 0, false, null);
    var queue2 = new Queue("test-queue-2", 10, 5, true, new Queue.RateLimit(2, 4.0));

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
          .body("[0].concurrency", equalTo(0))
          .body("[0].workerConcurrency", equalTo(0))
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
          .body("""
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
          .body("""
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
}
