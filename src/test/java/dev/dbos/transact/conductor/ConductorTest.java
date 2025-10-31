package dev.dbos.transact.conductor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import dev.dbos.transact.conductor.TestWebSocketServer.WebSocketTestListener;
import dev.dbos.transact.conductor.protocol.MessageType;
import dev.dbos.transact.conductor.protocol.SuccessResponse;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.utils.WorkflowStatusBuilder;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.net.InetAddress;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.WebSocket;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ClientHandshake;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
public class ConductorTest {

  private static final Logger logger = LoggerFactory.getLogger(ConductorTest.class);

  SystemDatabase mockDB;
  DBOSExecutor mockExec;
  Conductor.Builder builder;
  TestWebSocketServer testServer;

  static final ObjectMapper mapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

  @BeforeEach
  void beforeEach() throws Exception {
    testServer = new TestWebSocketServer(0);
    testServer.start();
    testServer.waitStart(1000);

    int port = testServer.getPort();
    assertTrue(port != 0, "Invalid Web Socket Server port");
    String domain = String.format("ws://localhost:%d", port);

    mockDB = mock(SystemDatabase.class);
    mockExec = mock(DBOSExecutor.class);
    when(mockExec.appName()).thenReturn("test-app-name");
    builder = new Conductor.Builder(mockExec, mockDB, "conductor-key").domain(domain);
  }

  @AfterEach
  void afterEach() throws Exception {
    testServer.stop();
  }

  @Test
  public void connectsToCorrectUrl() throws Exception {

    class Listener implements WebSocketTestListener {
      String resourceDescriptor;
      CountDownLatch latch = new CountDownLatch(1);

      @Override
      public void onOpen(WebSocket conn, ClientHandshake handshake) {
        resourceDescriptor = handshake.getResourceDescriptor();
        latch.countDown();
      }
    }

    Listener listener = new Listener();
    testServer.setListener(listener);

    try (Conductor conductor = builder.build()) {
      conductor.start();
      assertTrue(listener.latch.await(10, TimeUnit.SECONDS), "latch timed out");
      assertEquals("/websocket/test-app-name/conductor-key", listener.resourceDescriptor);
    }
  }

  @Test
  public void sendsPing() throws Exception {
    logger.info("sendsPing Starting");
    class Listener implements WebSocketTestListener {
      CountDownLatch latch = new CountDownLatch(3);
      boolean onCloseCalled = false;

      @Override
      public void onPing(WebSocket conn, Framedata frame) {
        WebSocketTestListener.super.onPing(conn, frame);
        latch.countDown();
      }

      @Override
      public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        this.onCloseCalled = true;
      }
    }

    Listener listener = new Listener();
    testServer.setListener(listener);

    builder.pingPeriodMs(2000).pingTimeoutMs(1000);
    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.latch.await(10, TimeUnit.SECONDS), "latch timed out");
      assertFalse(listener.onCloseCalled);
    } finally {
      logger.info("sendsPing ending");
    }
  }

  @Test
  public void reconnectsOnFailedPing() throws Exception {
    logger.info("reconnectsOnFailedPing Starting");
    class Listener implements WebSocketTestListener {
      int openCount = 0;
      CountDownLatch latch = new CountDownLatch(2);

      @Override
      public void onPing(WebSocket conn, Framedata frame) {
        // don't respond to pings
      }

      @Override
      public void onOpen(WebSocket conn, ClientHandshake handshake) {
        openCount++;
      }

      @Override
      public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        latch.countDown();
      }
    }

    Listener listener = new Listener();
    testServer.setListener(listener);

    builder.pingPeriodMs(2000).pingTimeoutMs(1000);
    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.latch.await(15, TimeUnit.SECONDS), "latch timed out");
      assertTrue(listener.openCount >= 2);
    } finally {
      logger.info("reconnectsOnFailedPing ending");
    }
  }

  @Test
  public void reconnectsOnRemoteClose() throws Exception {
    class Listener implements WebSocketTestListener {
      int closeCount = 0;
      CountDownLatch latch = new CountDownLatch(3);
      final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

      @Override
      public void onOpen(WebSocket conn, ClientHandshake handshake) {
        latch.countDown();
        if (latch.getCount() > 0) {
          scheduler.schedule(
              () -> {
                conn.close();
              },
              1,
              TimeUnit.SECONDS);
        }
      }

      @Override
      public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        closeCount++;
      }
    }

    Listener listener = new Listener();
    testServer.setListener(listener);

    builder.pingPeriodMs(2000).pingTimeoutMs(1000);
    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.latch.await(15, TimeUnit.SECONDS), "latch timed out");
      assertTrue(listener.closeCount >= 2);
    }
  }

  class MessageListener implements WebSocketTestListener {
    WebSocket webSocket;
    CountDownLatch openLatch = new CountDownLatch(1);
    String message;
    CountDownLatch messageLatch = new CountDownLatch(1);

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
      this.webSocket = conn;
      openLatch.countDown();
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
      this.message = message;
      messageLatch.countDown();
    }

    public void send(MessageType type, String requestId, Map<String, Object> fields)
        throws Exception {

      Map<String, Object> message = new HashMap<>(fields);
      message.put("type", Objects.requireNonNull(type).getValue());
      message.put("request_id", Objects.requireNonNull(requestId));

      String json = ConductorTest.mapper.writeValueAsString(message);
      this.webSocket.send(json);
    }
  }

  @Test
  public void canRecover() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    List<String> executorIds = List.of("exec1", "exec2", "exec3");

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("executor_ids", executorIds, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RECOVERY, "12345", message);
      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");

      // Verify that resumeWorkflow was called with the correct argument
      verify(mockExec).recoverPendingWorkflows(executorIds);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("recovery", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertNull(jsonNode.get("error_message"));
      assertTrue(jsonNode.get("success").asBoolean());
    }
  }

  @Test
  public void canRecoverThrows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    List<String> executorIds = List.of("exec1", "exec2", "exec3");
    String errorMessage = "canCancelThrows error";

    doThrow(new RuntimeException(errorMessage)).when(mockExec).recoverPendingWorkflows(executorIds);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("executor_ids", executorIds, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RECOVERY, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");

      // Verify that resumeWorkflow was called with the correct argument
      verify(mockExec).recoverPendingWorkflows(executorIds);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("recovery", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertEquals(errorMessage, jsonNode.get("error_message").asText());
      assertFalse(jsonNode.get("success").asBoolean());
    }
  }

  public void canExecutorInfo() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);

    String hostname = InetAddress.getLocalHost().getHostName();

    when(mockExec.appVersion()).thenReturn("test-app-version");
    when(mockExec.executorId()).thenReturn("test-executor-id");

    try (Conductor conductor = builder.build()) {
      conductor.start();
      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message = Map.of("unknown-field", "unknown-field-value");
      listener.send(MessageType.EXECUTOR_INFO, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("executor_info", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertEquals(hostname, jsonNode.get("hostname").asText());
      assertEquals("test-app-version", jsonNode.get("application_version").asText());
      assertEquals("test-executor-id", jsonNode.get("executor_id").asText());
      assertNull(jsonNode.get("error_message"));
    }
  }

  @Test
  public void canCancel() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String workflowId = "sample-wf-id";

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("workflow_id", workflowId, "unknown-field", "unknown-field-value");
      listener.send(MessageType.CANCEL, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");

      // Verify that resumeWorkflow was called with the correct argument
      verify(mockExec).cancelWorkflow(workflowId);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("cancel", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertNull(jsonNode.get("error_message"));
      assertTrue(jsonNode.get("success").asBoolean());
    }
  }

  @Test
  public void canCancelThrows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);

    String errorMessage = "canCancelThrows error";
    String workflowId = "sample-wf-id";

    doThrow(new RuntimeException(errorMessage)).when(mockExec).cancelWorkflow(anyString());

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("workflow_id", workflowId, "unknown-field", "unknown-field-value");
      listener.send(MessageType.CANCEL, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockExec).cancelWorkflow(workflowId);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("cancel", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertEquals(errorMessage, jsonNode.get("error_message").asText());
      assertFalse(jsonNode.get("success").asBoolean());
    }
  }

  @Test
  public void canResume() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String workflowId = "sample-wf-id";

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("workflow_id", workflowId, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RESUME, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockExec).resumeWorkflow(workflowId);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("resume", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertNull(jsonNode.get("error_message"));
      assertTrue(jsonNode.get("success").asBoolean());
    }
  }

  @Test
  public void canResumeThrows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);

    String errorMessage = "canResumeThrows error";
    String workflowId = "sample-wf-id";

    doThrow(new RuntimeException(errorMessage)).when(mockExec).resumeWorkflow(workflowId);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("workflow_id", workflowId, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RESUME, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockExec).resumeWorkflow(workflowId);

      SuccessResponse resp = mapper.readValue(listener.message, SuccessResponse.class);
      assertEquals("resume", resp.type);
      assertEquals("12345", resp.request_id);
      assertEquals(errorMessage, resp.error_message);
      assertFalse(resp.success);
    }
  }

  @Test
  public void canRestart() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String workflowId = "sample-wf-id";

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("workflow_id", workflowId, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RESTART, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockExec).forkWorkflow(eq(workflowId), eq(0), any());

      SuccessResponse resp = mapper.readValue(listener.message, SuccessResponse.class);
      assertEquals("restart", resp.type);
      assertEquals("12345", resp.request_id);
      assertTrue(resp.success);
      assertNull(resp.error_message);
    }
  }

  @Test
  public void canRestartThrows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);

    String workflowId = "sample-wf-id";
    String errorMessage = "canRestartThrows error";
    doThrow(new RuntimeException(errorMessage))
        .when(mockExec)
        .forkWorkflow(anyString(), anyInt(), any());

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("workflow_id", workflowId, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RESTART, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockExec).forkWorkflow(eq(workflowId), eq(0), any());

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("restart", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertEquals(errorMessage, jsonNode.get("error_message").asText());
      assertFalse(jsonNode.get("success").asBoolean());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void canFork() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String workflowId = "sample-wf-id";
    String newWorkflowId = "new-" + workflowId;

    var mockHandle = (WorkflowHandle<Object, Exception>) mock(WorkflowHandle.class);
    when(mockHandle.workflowId()).thenReturn(newWorkflowId);
    when(mockExec.forkWorkflow(eq(workflowId), anyInt(), any())).thenReturn(mockHandle);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(50000, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> body =
          Map.of(
              "workflow_id",
              workflowId,
              "start_step",
              2,
              "application_version",
              "appver-12345",
              "new_workflow_id",
              newWorkflowId,
              "unknown-field",
              "unknown-field-value");
      Map<String, Object> message = Map.of("body", body);
      listener.send(MessageType.FORK_WORKFLOW, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      ArgumentCaptor<ForkOptions> optionsCaptor = ArgumentCaptor.forClass(ForkOptions.class);
      verify(mockExec).forkWorkflow(eq(workflowId), eq(2), optionsCaptor.capture());
      ForkOptions capturedOptions = optionsCaptor.getValue();
      assertNotNull(capturedOptions);
      assertEquals("appver-12345", capturedOptions.applicationVersion());
      assertEquals(newWorkflowId, capturedOptions.forkedWorkflowId());
      assertEquals(null, capturedOptions.timeout());

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("fork_workflow", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertEquals(newWorkflowId, jsonNode.get("new_workflow_id").asText());
      assertNull(jsonNode.get("error_message"));
    }
  }

  @Test
  public void canForkThrow() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String workflowId = "sample-wf-id";

    String errorMessage = "canForkThrow error";
    doThrow(new RuntimeException(errorMessage))
        .when(mockExec)
        .forkWorkflow(eq(workflowId), anyInt(), any());

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> body =
          Map.of(
              "workflow_id",
              workflowId,
              "start_step",
              2,
              "application_version",
              "appver-12345",
              "new_workflow_id",
              "new-wf-id",
              "unknown-field",
              "unknown-field-value");
      Map<String, Object> message = Map.of("body", body);
      listener.send(MessageType.FORK_WORKFLOW, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      ArgumentCaptor<ForkOptions> optionsCaptor = ArgumentCaptor.forClass(ForkOptions.class);
      verify(mockExec).forkWorkflow(eq(workflowId), eq(2), optionsCaptor.capture());
      ForkOptions options = optionsCaptor.getValue();
      assertNotNull(options);
      assertEquals("appver-12345", options.applicationVersion());
      assertEquals("new-wf-id", options.forkedWorkflowId());
      assertEquals(null, options.timeout());

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("fork_workflow", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertNull(jsonNode.get("new_workflow_id"));
      assertEquals(errorMessage, jsonNode.get("error_message").asText());
    }
  }

  @Test
  public void canListWorkflows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
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

    when(mockExec.listWorkflows(any())).thenReturn(statuses);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> body =
          Map.of(
              "start_time", "2024-06-01T12:34:56Z",
              "workflow_name", "foobarbaz",
              "unknown-field", "unknown-field-value");
      Map<String, Object> message = Map.of("body", body);
      listener.send(MessageType.LIST_WORKFLOWS, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      ArgumentCaptor<ListWorkflowsInput> inputCaptor =
          ArgumentCaptor.forClass(ListWorkflowsInput.class);
      verify(mockExec).listWorkflows(inputCaptor.capture());
      ListWorkflowsInput input = inputCaptor.getValue();
      assertEquals(OffsetDateTime.parse("2024-06-01T12:34:56Z"), input.startTime());
      assertEquals("foobarbaz", input.workflowName());
      assertNull(input.limit());

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("list_workflows", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());

      JsonNode outputNode = jsonNode.get("output");
      assertNotNull(outputNode);
      assertTrue(outputNode.isArray());
      assertTrue(outputNode.size() == 3);

      assertEquals("wf-3", outputNode.get(2).get("WorkflowUUID").asText());
    }
  }

  @Test
  public void canListQueuedWorkflows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
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

    when(mockExec.listWorkflows(any())).thenReturn(statuses);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> body =
          Map.of(
              "start_time", "2024-06-01T12:34:56Z",
              "workflow_name", "foobarbaz",
              "unknown-field", "unknown-field-value");
      Map<String, Object> message = Map.of("body", body);
      listener.send(MessageType.LIST_QUEUED_WORKFLOWS, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      ArgumentCaptor<ListWorkflowsInput> inputCaptor =
          ArgumentCaptor.forClass(ListWorkflowsInput.class);
      verify(mockExec).listWorkflows(inputCaptor.capture());
      ListWorkflowsInput input = inputCaptor.getValue();
      assertEquals(OffsetDateTime.parse("2024-06-01T12:34:56Z"), input.startTime());
      assertEquals("foobarbaz", input.workflowName());
      assertNull(input.limit());

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("list_queued_workflows", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());

      JsonNode outputNode = jsonNode.get("output");
      assertNotNull(outputNode);
      assertTrue(outputNode.isArray());
      assertTrue(outputNode.size() == 3);

      assertEquals("wf-3", outputNode.get(2).get("WorkflowUUID").asText());
    }
  }

  @Test
  public void canGetWorkflow() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String workflowId = "sample-wf-id";

    WorkflowStatus status =
        new WorkflowStatusBuilder("wf-1")
            .status(WorkflowState.PENDING)
            .name("WF1")
            .createdAt(1754936102215L)
            .updatedAt(1754936102215L)
            .executorId("test-executor")
            .appVersion("test-app-ver")
            .appId("test-app-id")
            .build();

    when(mockDB.getWorkflowStatus(workflowId)).thenReturn(status);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message = Map.of("workflow_id", workflowId);
      listener.send(MessageType.GET_WORKFLOW, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockDB).getWorkflowStatus(workflowId);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("get_workflow", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      JsonNode outputNode = jsonNode.get("output");
      assertNotNull(outputNode);
      assertTrue(outputNode.isObject());
      assertEquals("wf-1", outputNode.get("WorkflowUUID").asText());
    }
  }

  @Test
  public void canExistPendingWorkflows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String executorId = "exec-id";
    String appVersion = "app-version";

    List<GetPendingWorkflowsOutput> outputs = new ArrayList<GetPendingWorkflowsOutput>();
    outputs.add(new GetPendingWorkflowsOutput("wf-1", null));
    outputs.add(new GetPendingWorkflowsOutput("wf-2", "queue"));

    when(mockDB.getPendingWorkflows(executorId, appVersion)).thenReturn(outputs);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("executor_id", executorId, "application_version", appVersion);
      listener.send(MessageType.EXIST_PENDING_WORKFLOWS, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockDB).getPendingWorkflows(executorId, appVersion);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("exist_pending_workflows", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertTrue(jsonNode.get("exist").asBoolean());
    }
  }

  @Test
  public void canExistPendingWorkflowsFalse() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String executorId = "exec-id";
    String appVersion = "app-version";

    List<GetPendingWorkflowsOutput> outputs = new ArrayList<GetPendingWorkflowsOutput>();
    when(mockDB.getPendingWorkflows(executorId, appVersion)).thenReturn(outputs);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of(
              "executor_id",
              executorId,
              "application_version",
              appVersion,
              "unknown-field",
              "unknown-field-value");
      listener.send(MessageType.EXIST_PENDING_WORKFLOWS, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockDB).getPendingWorkflows(executorId, appVersion);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("exist_pending_workflows", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertFalse(jsonNode.get("exist").asBoolean());
    }
  }

  @Test
  public void canListSteps() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);
    String workflowId = "workflow-id-1";

    List<StepInfo> steps = new ArrayList<StepInfo>();
    steps.add(new StepInfo(0, "function1", null, null, null));
    steps.add(new StepInfo(1, "function2", null, null, null));
    steps.add(new StepInfo(2, "function3", null, null, null));
    steps.add(new StepInfo(3, "function4", null, null, null));
    steps.add(new StepInfo(4, "function5", null, null, null));

    when(mockExec.listWorkflowSteps(workflowId)).thenReturn(steps);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> message =
          Map.of("workflow_id", workflowId, "unknown-field", "unknown-field-value");
      listener.send(MessageType.LIST_STEPS, "12345", message);

      assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
      verify(mockExec).listWorkflowSteps(workflowId);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("list_steps", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      JsonNode outputNode = jsonNode.get("output");
      assertNotNull(outputNode);
      assertTrue(outputNode.isArray());
      assertEquals(5, outputNode.size());
    }
  }

  @Test
  public void canRetention() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> body =
          Map.of(
              "gc_cutoff_epoch_ms",
              1L,
              "gc_rows_threshold",
              2L,
              "timeout_cutoff_epoch_ms",
              3L,
              "unknown-field",
              "unknown-field-value");
      Map<String, Object> message = Map.of("body", body, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RETENTION, "12345", message);

      assertTrue(listener.messageLatch.await(5, TimeUnit.SECONDS), "message latch timed out");
      verify(mockDB).garbageCollect(1L, 2L);
      verify(mockExec).globalTimeout(3L);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("retention", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertNull(jsonNode.get("error_message"));
      assertTrue(jsonNode.get("success").asBoolean());
    }
  }

  @Test
  public void canRetentionTimeoutNotSet() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      // Note, Map.of doesn't support null values
      Map<String, Object> body = new HashMap<>();
      body.put("gc_cutoff_epoch_ms", 1L);
      body.put("gc_rows_threshold", 2L);
      body.put("timeout_cutoff_epoch_ms", null);
      body.put("unknown-field", "unknown-field-value");

      Map<String, Object> message = Map.of("body", body, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RETENTION, "12345", message);

      assertTrue(listener.messageLatch.await(5, TimeUnit.SECONDS), "message latch timed out");
      verify(mockDB).garbageCollect(1L, 2L);
      verify(mockExec, never()).globalTimeout(anyLong());

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("retention", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertNull(jsonNode.get("error_message"));
      assertTrue(jsonNode.get("success").asBoolean());
    }
  }

  @Test
  public void canRetentionGcThrows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);

    String errorMessage = "canRetentionGcThrows error";
    doThrow(new RuntimeException(errorMessage)).when(mockDB).garbageCollect(anyLong(), anyLong());

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> body =
          Map.of(
              "gc_cutoff_epoch_ms",
              1L,
              "gc_rows_threshold",
              2L,
              "timeout_cutoff_epoch_ms",
              3L,
              "unknown-field",
              "unknown-field-value");
      Map<String, Object> message = Map.of("body", body, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RETENTION, "12345", message);

      assertTrue(listener.messageLatch.await(5, TimeUnit.SECONDS), "message latch timed out");
      verify(mockDB).garbageCollect(1L, 2L);
      verify(mockExec, never()).globalTimeout(anyLong());

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("retention", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertEquals(errorMessage, jsonNode.get("error_message").asText());
      assertFalse(jsonNode.get("success").asBoolean());
    }
  }

  @Test
  public void canRetentionTimeoutThrows() throws Exception {
    MessageListener listener = new MessageListener();
    testServer.setListener(listener);

    String errorMessage = "canRetentionTimeoutThrows error";
    doThrow(new RuntimeException(errorMessage)).when(mockExec).globalTimeout(anyLong());

    try (Conductor conductor = builder.build()) {
      conductor.start();

      assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

      Map<String, Object> body =
          Map.of(
              "gc_cutoff_epoch_ms",
              1L,
              "gc_rows_threshold",
              2L,
              "timeout_cutoff_epoch_ms",
              3L,
              "unknown-field",
              "unknown-field-value");
      Map<String, Object> message = Map.of("body", body, "unknown-field", "unknown-field-value");
      listener.send(MessageType.RETENTION, "12345", message);

      assertTrue(listener.messageLatch.await(5, TimeUnit.SECONDS), "message latch timed out");
      verify(mockDB).garbageCollect(1L, 2L);
      verify(mockExec).globalTimeout(3L);

      JsonNode jsonNode = mapper.readTree(listener.message);
      assertNotNull(jsonNode);
      assertEquals("retention", jsonNode.get("type").asText());
      assertEquals("12345", jsonNode.get("request_id").asText());
      assertEquals(errorMessage, jsonNode.get("error_message").asText());
      assertFalse(jsonNode.get("success").asBoolean());
    }
  }
}
