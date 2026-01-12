package dev.dbos.transact.admin;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminServer implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(AdminServer.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  static class DurationSerializer extends StdSerializer<Duration> {

    public DurationSerializer() {
      super(Duration.class);
    }

    @Override
    public void serialize(Duration value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeNumber(value.toMillis() / 1000.0);
    }
  }

  static {
    var module = new SimpleModule();
    module.addSerializer(Duration.class, new DurationSerializer());
    mapper.registerModule(module);
  }

  private HttpServer server;
  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;
  private final AtomicBoolean isRunning = new AtomicBoolean(true);

  public AdminServer(int port, DBOSExecutor exec, SystemDatabase sysDb) throws IOException {

    this.systemDatabase = sysDb;
    this.dbosExecutor = exec;

    Map<String, HttpHandler> staticRoutes = new HashMap<>();
    staticRoutes.put("/dbos-healthz", x -> healthCheck(x));
    staticRoutes.put("/dbos-workflow-recovery", x -> workflowRecovery(x)); // post
    staticRoutes.put("/deactivate", x -> deactivate(x));
    staticRoutes.put("/dbos-workflow-queues-metadata", x -> workflowQueuesMetadata(x));
    staticRoutes.put("/dbos-garbage-collect", x -> garbageCollect(x)); // post
    staticRoutes.put("/dbos-global-timeout", x -> globalTimeout(x)); // post
    staticRoutes.put("/queues", x -> listQueuedWorkflows(x));
    staticRoutes.put("/workflows", x -> listWorkflows(x));

    Map<String, WorkflowHandler> workflowRoutes = new HashMap<>();
    workflowRoutes.put("/steps", (x, wfid) -> listSteps(x, wfid));
    workflowRoutes.put("/cancel", (x, wfid) -> cancel(x, wfid));
    workflowRoutes.put("/resume", (x, wfid) -> resume(x, wfid));
    workflowRoutes.put("/fork", (x, wfid) -> fork(x, wfid));

    server = HttpServer.create(new InetSocketAddress(port), 0);
    Pattern workflowPattern = Pattern.compile("/workflows/([^/]+)(/[^/]*)?");

    server.createContext(
        "/",
        exchange -> {
          try {
            var path = exchange.getRequestURI().getPath();
            var handler = staticRoutes.get(path);
            if (handler != null) {
              handler.handle(exchange);
              return;
            }

            var matcher = workflowPattern.matcher(path);
            if (matcher.matches()) {
              var workflowId = matcher.group(1);
              var subPath = matcher.group(2);

              if (subPath == null) {
                getWorkflow(exchange, workflowId);
                return;
              }

              var wfhandler = workflowRoutes.get(subPath);
              if (wfhandler != null) {
                wfhandler.handle(exchange, workflowId);
                return;
              }
            }

            exchange.sendResponseHeaders(404, -1);
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
            sendText(exchange, 500, e.getMessage());
          }
        });
  }

  public void start() {
    logger.debug("start");
    server.start();
  }

  public void stop() {
    logger.debug("stop");
    server.stop(0);
  }

  @Override
  public void close() {
    stop();
  }

  private void healthCheck(HttpExchange exchange) throws IOException {
    sendJson(
        exchange,
        200,
        """
        {"status":"healthy"}""");
  }

  private void workflowRecovery(HttpExchange exchange) throws IOException {
    if (!ensurePostJson(exchange)) return;

    List<String> executorIds =
        mapper.readValue(exchange.getRequestBody(), new TypeReference<>() {});
    logger.debug("workflowRecovery executors {}", executorIds);
    var handles = dbosExecutor.recoverPendingWorkflows(executorIds);
    List<String> workflowIds =
        handles.stream().map(WorkflowHandle::workflowId).collect(Collectors.toList());
    sendMappedJson(exchange, 200, workflowIds);
  }

  private void deactivate(HttpExchange exchange) throws IOException {
    if (isRunning.compareAndSet(true, false)) {
      logger.info(
          "deactivate executor {} app version {}",
          dbosExecutor.executorId(),
          dbosExecutor.appVersion());
      dbosExecutor.deactivateLifecycleListeners();
    }
    sendText(exchange, 200, "deactivated");
  }

  private void workflowQueuesMetadata(HttpExchange exchange) throws IOException {
    var queues = dbosExecutor.getQueues();
    sendMappedJson(exchange, 200, queues);
  }

  private void garbageCollect(HttpExchange exchange) throws IOException {
    if (!ensurePostJson(exchange)) return;

    var request = mapper.readValue(exchange.getRequestBody(), GarbageCollectRequest.class);

    systemDatabase.garbageCollect(request.cutoff_epoch_timestamp_ms, (long) request.rows_threshold);

    exchange.sendResponseHeaders(204, 0);
  }

  private void globalTimeout(HttpExchange exchange) throws IOException {
    if (!ensurePostJson(exchange)) return;

    var request = mapper.readValue(exchange.getRequestBody(), GlobalTimeoutRequest.class);
    dbosExecutor.globalTimeout(request.cutoff_epoch_timestamp_ms);

    exchange.sendResponseHeaders(204, 0);
  }

  private void listWorkflows(HttpExchange exchange) throws IOException {
    if (!ensurePostJson(exchange)) return;

    var request = mapper.readValue(exchange.getRequestBody(), ListWorkflowsRequest.class);
    var input = request.asInput();
    var workflows = systemDatabase.listWorkflows(input);
    var response = workflows.stream().map(WorkflowsOutput::of).collect(Collectors.toList());
    sendMappedJson(exchange, 200, response);
  }

  private void listQueuedWorkflows(HttpExchange exchange) throws IOException {
    if (!ensurePostJson(exchange)) return;

    var request = mapper.readValue(exchange.getRequestBody(), ListQueuedWorkflowsRequest.class);
    var input = request.asInput();
    var workflows = systemDatabase.listWorkflows(input);
    var response = workflows.stream().map(WorkflowsOutput::of).collect(Collectors.toList());
    sendMappedJson(exchange, 200, response);
  }

  private void getWorkflow(HttpExchange exchange, String wfid) throws IOException {
    var input = new ListWorkflowsInput().withWorkflowId(wfid);
    var workflows = systemDatabase.listWorkflows(input);
    if (workflows.size() == 0) {
      sendText(exchange, 404, "Workflow not found");
      return;
    }

    var response = WorkflowsOutput.of(workflows.get(0));
    sendMappedJson(exchange, 200, response);
  }

  private void listSteps(HttpExchange exchange, String wfid) throws IOException {
    var steps = systemDatabase.listWorkflowSteps(wfid);
    var response = steps.stream().map(StepOutput::of).collect(Collectors.toList());
    sendMappedJson(exchange, 200, response);
  }

  private void cancel(HttpExchange exchange, String wfid) throws IOException {
    if (!ensurePost(exchange)) return;

    logger.info("cancel workflow {}", wfid);

    dbosExecutor.cancelWorkflow(wfid);
    exchange.sendResponseHeaders(204, 0);
  }

  private void resume(HttpExchange exchange, String wfid) throws IOException {
    if (!ensurePost(exchange)) return;

    logger.info("resume workflow {}", wfid);

    dbosExecutor.resumeWorkflow(wfid);
    exchange.sendResponseHeaders(204, 0);
  }

  private void fork(HttpExchange exchange, String wfid) throws IOException {
    if (!ensurePostJson(exchange)) return;

    var request = mapper.readValue(exchange.getRequestBody(), ForkRequest.class);
    int startStep = request.start_step == null ? 0 : request.start_step;
    var options = new ForkOptions(request.new_workflow_id, request.application_version, null);

    logger.info("fork workflow {} step {}", wfid, startStep);
    var handle = dbosExecutor.forkWorkflow(wfid, startStep, options);
    var response = new ForkResponse(handle.workflowId());
    sendMappedJson(exchange, 200, response);
  }

  private static void sendText(HttpExchange exchange, int statusCode, String text)
      throws IOException {
    exchange.getResponseHeaders().add("Content-Type", "text/plain");
    byte[] bytes = text.getBytes();
    exchange.sendResponseHeaders(statusCode, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private static void sendJson(HttpExchange exchange, int statusCode, String json)
      throws IOException {
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    byte[] bytes = json.getBytes();
    exchange.sendResponseHeaders(statusCode, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private static void sendMappedJson(HttpExchange exchange, int statusCode, Object json)
      throws IOException {
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    byte[] bytes = mapper.writeValueAsBytes(json);
    exchange.sendResponseHeaders(statusCode, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private static boolean ensurePost(HttpExchange exchange) throws IOException {
    // Check method
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, -1); // Method Not Allowed
      return false;
    }
    return true;
  }

  private static boolean ensurePostJson(HttpExchange exchange) throws IOException {
    // Check method
    if (!ensurePost(exchange)) {
      return false;
    }

    // Check Content-Type
    String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
    if (contentType == null || !contentType.startsWith("application/json")) {
      String response = "Unsupported Media Type";
      exchange.sendResponseHeaders(415, response.getBytes().length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
      return false;
    }

    return true; // all good
  }

  @FunctionalInterface
  interface WorkflowHandler {
    void handle(HttpExchange exchange, String workflowId) throws IOException;
  }

  record GarbageCollectRequest(long cutoff_epoch_timestamp_ms, int rows_threshold) {}

  record GlobalTimeoutRequest(long cutoff_epoch_timestamp_ms) {}

  record ForkRequest(Integer start_step, String new_workflow_id, String application_version) {}

  record ForkResponse(String workflow_id) {}
}
