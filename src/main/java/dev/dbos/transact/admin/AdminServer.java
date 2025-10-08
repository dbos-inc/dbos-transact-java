package dev.dbos.transact.admin;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminServer implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(AdminServer.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private HttpServer server;
  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;

  public AdminServer(int port, DBOSExecutor exec, SystemDatabase sysDb) throws IOException {

    this.systemDatabase = sysDb;
    this.dbosExecutor = exec;

    Map<String, HttpHandler> staticRoutes = new HashMap<>();
    staticRoutes.put("/dbos-healthz", x -> healthCheck(x));
    staticRoutes.put("/dbos-workflow-recovery", x -> workflowRecovery(x)); // post
    staticRoutes.put("/dbos-deactivate", x -> deactivate(x));
    staticRoutes.put("/dbos-workflow-queues-metadata", x -> workflowQueuesMetadata(x));
    staticRoutes.put("/dbos-garbage-collect", x -> garbageCollect(x)); // post
    staticRoutes.put("/dbos-global-timeout", x -> globalTimeout(x)); // post
    staticRoutes.put("/queues", x -> listQueuedWorkflows(x));
    staticRoutes.put("/workflows", x -> listWorkflows(x));

    Map<String, WorkflowHandler> workflowRoutes = new HashMap<>();
    workflowRoutes.put("steps", (x, wfid) -> listSteps(x, wfid));
    workflowRoutes.put("cancel", (x, wfid) -> cancel(x, wfid));
    workflowRoutes.put("restart", (x, wfid) -> restart(x, wfid));
    workflowRoutes.put("resume", (x, wfid) -> resume(x, wfid));
    workflowRoutes.put("fork", (x, wfid) -> fork(x, wfid));

    server = HttpServer.create(new InetSocketAddress(port), 0);
    Pattern workflowPattern = Pattern.compile("/workflows/([^/]+)(/[^/]*)?");

    server.createContext(
        "/",
        exchange -> {
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
        });
  }

  public void start() {
    server.start();
  }

  public void stop() {
    server.stop(0);
  }

  @Override
  public void close() {
    stop();
  }

  private void healthCheck(HttpExchange exchange) throws IOException {
    sendJson(exchange, 200, """
        {"status":"healthy"}""");
  }

  private void workflowRecovery(HttpExchange exchange) throws IOException {
    if (!ensurePostJson(exchange)) return;

    List<String> executorIds =
        mapper.readValue(exchange.getRequestBody(), new TypeReference<List<String>>() {});
    logger.debug("Recovering workflows for executors {}", executorIds);
    var handles = dbosExecutor.recoverPendingWorkflows(executorIds);
    List<String> workflowIds =
        handles.stream().map(h -> h.getWorkflowId()).collect(Collectors.toList());
    sendMappedJson(exchange, 200, workflowIds);
  }

  private void deactivate(HttpExchange exchange) throws IOException {
    sendText(exchange, 500, "not implemented");
  }

  private void workflowQueuesMetadata(HttpExchange x) throws IOException {}

  private void garbageCollect(HttpExchange exchange) throws IOException {}

  private void globalTimeout(HttpExchange x) throws IOException {}

  private void listQueuedWorkflows(HttpExchange x) throws IOException {}

  private void getWorkflow(HttpExchange x, String wfid) throws IOException {}

  private void cancel(HttpExchange x, String wfid) throws IOException {}

  private void fork(HttpExchange x, String wfid) throws IOException {}

  private void resume(HttpExchange x, String wfid) throws IOException {}

  private void restart(HttpExchange x, String wfid) throws IOException {}

  private void listSteps(HttpExchange x, String wfid) throws IOException {}

  private void listWorkflows(HttpExchange x) throws IOException {}

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

  private static boolean ensurePostJson(HttpExchange exchange) throws IOException {
    // Check method
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, -1); // Method Not Allowed
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

  record RateLimitMetadata(Integer limit, Double period) {}

  record QueueMetadata(
      String name,
      int concurrency,
      int workerConcurrency,
      boolean priorityEnabled,
      RateLimitMetadata rateLimit,
      int maxTasksPerIteration) {}
}
