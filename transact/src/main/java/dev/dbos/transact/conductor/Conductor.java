package dev.dbos.transact.conductor;

import dev.dbos.transact.conductor.protocol.*;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Conductor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(Conductor.class);

  private final int pingPeriodMs;
  private final int pingTimeoutMs;
  private final int reconnectDelayMs;

  private final String url;
  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final HttpClient httpClient;

  private final AtomicReference<WebSocket> webSocket = new AtomicReference<>();
  private final AtomicReference<CompletableFuture<WebSocket>> connectFuture =
      new AtomicReference<>();
  private ScheduledFuture<?> pingInterval;
  private ScheduledFuture<?> pingTimeout;
  private ScheduledFuture<?> reconnectTimeout;

  private Conductor(Builder builder) {
    Objects.requireNonNull(builder.systemDatabase, "SystemDatabase must not be null");
    Objects.requireNonNull(builder.dbosExecutor, "DBOSExecutor must not be null");
    Objects.requireNonNull(builder.conductorKey, "Conductor key must not be null");

    this.systemDatabase = builder.systemDatabase;
    this.dbosExecutor = builder.dbosExecutor;

    String appName = dbosExecutor.appName();
    Objects.requireNonNull(appName, "App Name must not be null to use Conductor");

    String domain = builder.domain;
    if (domain == null) {
      String dbosDomain = System.getenv("DBOS_DOMAIN");
      if (dbosDomain == null || dbosDomain.trim().isEmpty()) {
        domain = "wss://cloud.dbos.dev";
      } else {
        domain = "wss://" + dbosDomain.trim();
      }
      domain += "/conductor/v1alpha1";
    } else {
      // ensure there is no trailing slash
      domain = domain.replaceAll("/$", "");
    }

    this.url = domain + "/websocket/" + appName + "/" + builder.conductorKey;

    this.pingPeriodMs = builder.pingPeriodMs;
    this.pingTimeoutMs = builder.pingTimeoutMs;
    this.reconnectDelayMs = builder.reconnectDelayMs;
    this.httpClient = buildHttpClient();
  }

  // TODO: do we need the insecure connection?
  private static HttpClient buildHttpClient() {
    try {
      // Intentionally insecure: matches previous Netty InsecureTrustManagerFactory behavior
      TrustManager[] trustAllCerts =
          new TrustManager[] {
            new X509TrustManager() {
              @Override
              public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
              }

              @Override
              public void checkClientTrusted(X509Certificate[] certs, String authType) {}

              @Override
              public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }
          };
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustAllCerts, new SecureRandom());
      return HttpClient.newBuilder().sslContext(sslContext).build();
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to create HttpClient", e);
    }
  }

  private class WebSocketListener implements WebSocket.Listener {
    private final StringBuilder messageBuffer = new StringBuilder();
    private ImportFrameQueue importFrameQueue = null;

    private void closePipe() {
      if (importFrameQueue != null) {
        importFrameQueue.finish();
        importFrameQueue = null;
      }
    }

    @Override
    public void onOpen(WebSocket ws) {
      logger.info("Successfully established websocket connection to DBOS conductor at {}", url);
      webSocket.set(ws);
      ws.request(1);
      setPingInterval(ws);
    }

    @Override
    public CompletableFuture<?> onText(WebSocket ws, CharSequence data, boolean last) {
      logger.trace("onText data size {} last {}", data.length(), last);

      // Streaming import path: queue each frame for the worker thread (non-blocking)
      if (importFrameQueue != null) {
        try {
          importFrameQueue.addFrame(data);
        } catch (Exception e) {
          logger.error("Error queuing frame for import; streaming import aborted", e);
          closePipe();
        }
        if (last) {
          closePipe();
        }
        ws.request(1);
        return null;
      }

      // Detect import_workflow on the first frame of a new message
      if (messageBuffer.length() == 0 && isImportMessage(data)) {
        logger.debug("import message detected");

        importFrameQueue = new ImportFrameQueue();
        importFrameQueue.addFrame(data);
        streamImportAsync(Conductor.this, ws, importFrameQueue);
        logger.debug("streamImportAsync started");
        if (last) {
          closePipe();
        }

        ws.request(1);
        return null;
      }

      messageBuffer.append(data);
      if (!last) {
        ws.request(1);
        return null;
      }

      String messageText = messageBuffer.toString();
      messageBuffer.setLength(0);
      int messageSize = messageText.length();

      // Allow the next message to be delivered before we finish processing this one
      ws.request(1);

      logger.debug("Received {} chars from Conductor {}", messageSize, ws.getClass().getName());

      BaseMessage request;
      try (InputStream is =
          new ByteArrayInputStream(messageText.getBytes(StandardCharsets.UTF_8))) {
        request = JSONUtil.fromJson(is, BaseMessage.class);
      } catch (Exception e) {
        logger.error("Conductor JSON Parsing error for {} char message", messageSize, e);
        return null;
      }

      try {
        long startTime = System.currentTimeMillis();
        logger.info(
            "Processing conductor request: type={}, id={}", request.type, request.request_id);

        final BaseMessage finalRequest = request;
        getResponseAsync(request, ws)
            .whenComplete(
                (response, throwable) -> {
                  try {
                    long processingTime = System.currentTimeMillis() - startTime;
                    if (throwable != null) {
                      logger.error(
                          "Error processing request: type={}, id={}, duration={}ms",
                          finalRequest.type,
                          finalRequest.request_id,
                          processingTime,
                          throwable);

                      // Create an error response
                      BaseResponse errorResponse =
                          new BaseResponse(
                              finalRequest.type, finalRequest.request_id, throwable.getMessage());
                      writeFragmentedResponse(ws, errorResponse);
                    } else if (response != null) {
                      // null response means the handler already sent the response directly
                      logger.info(
                          "Completed processing request: type={}, id={}, duration={}ms",
                          finalRequest.type,
                          finalRequest.request_id,
                          processingTime);
                      writeFragmentedResponse(ws, response);
                    }
                  } catch (Exception e) {
                    logger.error(
                        "Error writing response for request type={}, id={}",
                        finalRequest.type,
                        finalRequest.request_id,
                        e);
                  }
                });
      } catch (Exception e) {
        logger.error(
            "Conductor Response error for request type={}, id={}",
            request.type,
            request.request_id,
            e);
      }

      return null;
    }

    @Override
    public CompletableFuture<?> onPing(WebSocket ws, ByteBuffer message) {
      logger.debug("Received ping from conductor");
      ws.sendPong(message);
      ws.request(1);
      return null;
    }

    @Override
    public CompletableFuture<?> onPong(WebSocket ws, ByteBuffer message) {
      logger.debug("Received pong from conductor");
      if (pingTimeout != null) {
        pingTimeout.cancel(false);
        pingTimeout = null;
        logger.debug("Cancelled ping timeout - connection is healthy");
      } else {
        logger.debug("Received pong but no ping timeout was active");
      }
      ws.request(1);
      return null;
    }

    @Override
    public CompletableFuture<?> onClose(WebSocket ws, int statusCode, String reason) {
      logger.warn(
          "Received close frame from conductor: status={}, reason='{}'", statusCode, reason);
      if (isShutdown.get()) {
        logger.debug("Shutdown Conductor connection");
      } else if (reconnectTimeout == null) {
        logger.warn("onClose: Connection to conductor lost. Reconnecting");
        resetWebSocket();
      }
      return null;
    }

    @Override
    public void onError(WebSocket ws, Throwable error) {
      logger.warn(
          "Unexpected exception in websocket connection to conductor. WebSocket active: {}",
          !ws.isInputClosed(),
          error);
      resetWebSocket();
    }
  }

  /**
   * A Reader backed by an unbounded queue of CharSequence frames. The producer side (onText) calls
   * addFrame/finish which never block. The consumer side (streamImportAsync) reads data normally,
   * blocking only when waiting for more frames to arrive. This avoids blocking the WebSocket I/O
   * thread, which would prevent the TCP receive buffer from draining and cause the conductor's pong
   * writes to time out.
   */
  private static class ImportFrameQueue extends Reader {
    private final LinkedBlockingQueue<CharSequence> frames = new LinkedBlockingQueue<>();
    private final AtomicBoolean done = new AtomicBoolean(false);
    private CharSequence current = null;
    private int pos = 0;

    void addFrame(CharSequence data) {
      frames.add(data);
    }

    void finish() {
      if (done.compareAndSet(false, true)) {
        frames.add(""); // wake up a blocked reader
      }
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      while (true) {
        if (current != null && pos < current.length()) {
          int n = Math.min(len, current.length() - pos);
          for (int i = 0; i < n; i++) {
            cbuf[off + i] = current.charAt(pos + i);
          }
          pos += n;
          return n;
        }
        try {
          CharSequence next = frames.poll(100, TimeUnit.MILLISECONDS);
          if (next == null) {
            if (done.get() && frames.isEmpty()) return -1;
            continue;
          }
          if (next.length() == 0 && done.get()) return -1;
          current = next;
          pos = 0;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted reading import stream", e);
        }
      }
    }

    @Override
    public void close() {
      finish();
    }
  }

  private static void writeFragmentedResponse(WebSocket ws, BaseResponse response)
      throws Exception {
    int fragmentSize = 128 * 1024; // 128k
    logger.debug(
        "Starting to write fragmented response: type={}, id={}",
        response.type,
        response.request_id);
    synchronized (ws) {
      try (OutputStream out = new FragmentingOutputStream(ws, fragmentSize)) {
        JSONUtil.toJsonStream(response, out);
      }
    }
    logger.debug(
        "Completed writing fragmented response: type={}, id={}",
        response.type,
        response.request_id);
  }

  private static class FragmentingOutputStream extends OutputStream {
    private final WebSocket ws;
    private final int fragmentSize;
    private final byte[] buffer;
    private int bufferPos = 0;
    private boolean closed = false;

    FragmentingOutputStream(WebSocket ws, int fragmentSize) {
      this.ws = ws;
      this.fragmentSize = fragmentSize;
      this.buffer = new byte[fragmentSize];
      logger.debug("Created FragmentingOutputStream with fragment size: {}", fragmentSize);
    }

    @Override
    public void write(int b) throws IOException {
      buffer[bufferPos++] = (byte) b;
      if (bufferPos == fragmentSize) {
        flushBuffer(false);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      while (len > 0) {
        int toCopy = Math.min(len, fragmentSize - bufferPos);
        System.arraycopy(b, off, buffer, bufferPos, toCopy);
        bufferPos += toCopy;
        off += toCopy;
        len -= toCopy;
        if (bufferPos == fragmentSize) {
          flushBuffer(false);
        }
      }
    }

    private void flushBuffer(boolean last) throws IOException {
      if (bufferPos == 0 && !last) {
        return;
      }
      String chunk = new String(buffer, 0, bufferPos, StandardCharsets.UTF_8);
      int frameSize = bufferPos;
      bufferPos = 0;
      try {
        ws.sendText(chunk, last).join();
      } catch (Exception e) {
        logger.error("Failed to send websocket frame: {} bytes", frameSize, e);
        throw new IOException("Failed to send websocket frame", e);
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        flushBuffer(true);
        closed = true;
      }
    }
  }

  public static class Builder {
    private SystemDatabase systemDatabase;
    private DBOSExecutor dbosExecutor;
    private String conductorKey;
    private String domain;
    private int pingPeriodMs = 20000;
    private int pingTimeoutMs = 15000;
    private int reconnectDelayMs = 1000;

    public Builder(DBOSExecutor e, SystemDatabase s, String key) {
      systemDatabase = s;
      dbosExecutor = e;
      conductorKey = key;
    }

    public Builder domain(String domain) {
      this.domain = domain;
      return this;
    }

    // timing fields are package public for tests
    Builder pingPeriodMs(int pingPeriodMs) {
      this.pingPeriodMs = pingPeriodMs;
      return this;
    }

    Builder pingTimeoutMs(int pingTimeoutMs) {
      this.pingTimeoutMs = pingTimeoutMs;
      return this;
    }

    Builder reconnectDelayMs(int reconnectDelayMs) {
      this.reconnectDelayMs = reconnectDelayMs;
      return this;
    }

    public Conductor build() {
      return new Conductor(this);
    }
  }

  @Override
  public void close() {
    this.stop();
  }

  public void start() {
    logger.debug("start");
    connectWebSocket();
  }

  public void stop() {
    logger.debug("stop");
    if (isShutdown.compareAndSet(false, true)) {
      if (pingInterval != null) {
        pingInterval.cancel(true);
      }
      if (pingTimeout != null) {
        pingTimeout.cancel(true);
      }
      if (reconnectTimeout != null) {
        reconnectTimeout.cancel(true);
      }

      scheduler.shutdownNow();

      CompletableFuture<WebSocket> cf = connectFuture.getAndSet(null);
      if (cf != null) {
        cf.cancel(true);
      }

      WebSocket ws = webSocket.getAndSet(null);
      if (ws != null) {
        ws.abort();
      }
    }
  }

  void setPingInterval(WebSocket ws) {
    logger.debug("setPingInterval");

    if (pingInterval != null) {
      pingInterval.cancel(false);
    }
    pingInterval =
        scheduler.scheduleAtFixedRate(
            () -> {
              if (this.isShutdown.get()) {
                return;
              }
              try {
                if (ws.isOutputClosed()) {
                  logger.debug("websocket not active, NOT sending ping to conductor");
                  return;
                }

                logger.debug("Sending ping to conductor (timeout in {}ms)", pingTimeoutMs);
                ws.sendPing(ByteBuffer.allocate(0))
                    .whenComplete(
                        (w, e) -> {
                          if (e != null) {
                            logger.error("Failed to send ping to conductor", e);
                            resetWebSocket();
                          }
                        });

                pingTimeout =
                    scheduler.schedule(
                        () -> {
                          if (!isShutdown.get()) {
                            logger.error(
                                "Ping timeout after {}ms - no pong received from conductor."
                                    + " Connection lost, reconnecting.",
                                pingTimeoutMs);
                            resetWebSocket();
                          }
                        },
                        pingTimeoutMs,
                        TimeUnit.MILLISECONDS);
              } catch (Exception e) {
                logger.error("setPingInterval::scheduleAtFixedRate", e);
              }
            },
            0,
            pingPeriodMs,
            TimeUnit.MILLISECONDS);
  }

  void resetWebSocket() {
    logger.info(
        "Resetting websocket connection. WebSocket: {}",
        webSocket.get() != null ? "connected" : "null");

    if (pingInterval != null) {
      pingInterval.cancel(false);
      pingInterval = null;
    }

    if (pingTimeout != null) {
      pingTimeout.cancel(false);
      pingTimeout = null;
    }

    CompletableFuture<WebSocket> cf = connectFuture.getAndSet(null);
    if (cf != null) {
      cf.cancel(true);
    }

    WebSocket ws = webSocket.getAndSet(null);
    if (ws != null) {
      ws.abort();
    }

    if (isShutdown.get()) {
      logger.debug("Not scheduling reconnection - conductor is shutting down");
      return;
    }

    if (reconnectTimeout == null) {
      logger.info("Scheduling websocket reconnection in {}ms", reconnectDelayMs);
      reconnectTimeout =
          scheduler.schedule(
              () -> {
                reconnectTimeout = null;
                logger.info("Attempting websocket reconnection");
                connectWebSocket();
              },
              reconnectDelayMs,
              TimeUnit.MILLISECONDS);
    } else {
      logger.debug("Reconnection already scheduled");
    }
  }

  void connectWebSocket() {
    if (webSocket.get() != null) {
      logger.warn("Conductor websocket already exists");
      return;
    }

    if (isShutdown.get()) {
      logger.debug("Not connecting web socket as conductor is shutting down");
      return;
    }

    try {
      logger.debug("Connecting to conductor at {}", url);
      URI uri = new URI(url);

      if (connectFuture.get() != null) {
        logger.debug("Already connecting to conductor");
        return;
      }

      var future = httpClient.newWebSocketBuilder().buildAsync(uri, new WebSocketListener());

      if (!connectFuture.compareAndSet(null, future)) {
        // Lost the race — another connection attempt started between the check and the CAS.
        logger.debug("Already connecting to conductor");
        future.cancel(true);
        return;
      }

      future.whenComplete(
          (ws, e) -> {
            connectFuture.set(null);
            if (e != null) {
              logger.warn("Failed to connect to conductor at {}. Reconnecting", url, e);
              resetWebSocket();
            }
            // On success: onOpen has already been called and set webSocket
          });

    } catch (Exception e) {
      logger.warn("Error in conductor loop. Reconnecting", e);
      resetWebSocket();
    }
  }

  CompletableFuture<BaseResponse> getResponseAsync(BaseMessage message, WebSocket ws) {
    logger.debug("getResponseAsync {}", message.type);
    MessageType messageType = MessageType.fromValue(message.type);
    if (messageType == null) {
      logger.warn("Conductor unknown message type {}", message.type);
      return CompletableFuture.completedFuture(
          new BaseResponse(message.type, message.request_id, "Unknown message type"));
    }
    return switch (messageType) {
      case ALERT -> handleAlert(this, message);
      case BACKFILL_SCHEDULE -> handleBackfillSchedule(this, message);
      case CANCEL -> handleCancel(this, message);
      case DELETE -> handleDelete(this, message);
      case EXECUTOR_INFO -> handleExecutorInfo(this, message);
      case EXIST_PENDING_WORKFLOWS -> handleExistPendingWorkflows(this, message);
      case EXPORT_WORKFLOW -> handleExportWorkflow(this, message, ws);
      case FORK_WORKFLOW -> handleFork(this, message);
      case GET_METRICS -> handleGetMetrics(this, message);
      case GET_SCHEDULE -> handleGetSchedule(this, message);
      case GET_WORKFLOW -> handleGetWorkflow(this, message);
      case IMPORT_WORKFLOW -> handleImportWorkflow(this, message);
      case LIST_APPLICATION_VERSIONS -> handleListApplicationVersions(this, message);
      case LIST_QUEUED_WORKFLOWS -> handleListQueuedWorkflows(this, message);
      case LIST_SCHEDULES -> handleListSchedules(this, message);
      case LIST_STEPS -> handleListSteps(this, message);
      case LIST_WORKFLOWS -> handleListWorkflows(this, message);
      case PAUSE_SCHEDULE -> handlePauseSchedule(this, message);
      case RECOVERY -> handleRecovery(this, message);
      case RESTART -> handleRestart(this, message);
      case RESUME -> handleResume(this, message);
      case RESUME_SCHEDULE -> handleResumeSchedule(this, message);
      case RETENTION -> handleRetention(this, message);
      case SET_LATEST_APPLICATION_VERSION -> handleSetLatestApplicationVersion(this, message);
      case TRIGGER_SCHEDULE -> handleTriggerSchedule(this, message);
    };
  }

  static CompletableFuture<BaseResponse> handleExecutorInfo(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String hostname = InetAddress.getLocalHost().getHostName();
            return new ExecutorInfoResponse(
                message,
                conductor.dbosExecutor.executorId(),
                conductor.dbosExecutor.appVersion(),
                hostname);
          } catch (Exception e) {
            return new ExecutorInfoResponse(message, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleRecovery(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          RecoveryRequest request = (RecoveryRequest) message;
          try {
            conductor.dbosExecutor.recoverPendingWorkflows(request.executor_ids);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error("Exception encountered when recovering pending workflows", e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleCancel(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          CancelRequest request = (CancelRequest) message;
          List<String> ids =
              (request.workflow_ids != null && !request.workflow_ids.isEmpty())
                  ? request.workflow_ids
                  : List.of(request.workflow_id);
          try {
            conductor.dbosExecutor.cancelWorkflows(ids);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error("Exception encountered when cancelling workflow(s) {}", ids, e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleDelete(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          DeleteRequest request = (DeleteRequest) message;
          List<String> ids =
              (request.workflow_ids != null && !request.workflow_ids.isEmpty())
                  ? request.workflow_ids
                  : List.of(request.workflow_id);
          try {
            conductor.systemDatabase.deleteWorkflows(ids, request.delete_children);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error("Exception encountered when deleting workflow(s) {}", ids, e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleResume(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ResumeRequest request = (ResumeRequest) message;
          List<String> ids =
              (request.workflow_ids != null && !request.workflow_ids.isEmpty())
                  ? request.workflow_ids
                  : List.of(request.workflow_id);
          try {
            conductor.dbosExecutor.resumeWorkflows(ids);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error("Exception encountered when resuming workflow(s) {}", ids, e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleRestart(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          RestartRequest request = (RestartRequest) message;
          try {
            ForkOptions options = new ForkOptions();
            conductor.dbosExecutor.forkWorkflow(request.workflow_id, 0, options);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error(
                "Exception encountered when restarting workflow {}", request.workflow_id, e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleFork(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ForkWorkflowRequest request = (ForkWorkflowRequest) message;
          if (request.body.workflow_id == null || request.body.start_step == null) {
            return new ForkWorkflowResponse(request, null, "Invalid Fork Workflow Request");
          }
          try {
            var options =
                new ForkOptions(
                    request.body.new_workflow_id, request.body.application_version, null);
            WorkflowHandle<?, ?> handle =
                conductor.dbosExecutor.forkWorkflow(
                    request.body.workflow_id, request.body.start_step, options);
            return new ForkWorkflowResponse(request, handle.workflowId());
          } catch (Exception e) {
            logger.error("Exception encountered when forking workflow {}", request, e);
            return new ForkWorkflowResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleListWorkflows(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ListWorkflowsRequest request = (ListWorkflowsRequest) message;
          try {
            ListWorkflowsInput input = request.asInput();
            List<WorkflowStatus> statuses = conductor.dbosExecutor.listWorkflows(input);
            List<WorkflowsOutput> output =
                statuses.stream().map(s -> new WorkflowsOutput(s)).collect(Collectors.toList());
            return new WorkflowOutputsResponse(request, output);
          } catch (Exception e) {
            logger.error("Exception encountered when listing workflows", e);
            return new WorkflowOutputsResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleListQueuedWorkflows(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ListQueuedWorkflowsRequest request = (ListQueuedWorkflowsRequest) message;
          try {
            ListWorkflowsInput input = request.asInput();
            List<WorkflowStatus> statuses = conductor.dbosExecutor.listWorkflows(input);
            List<WorkflowsOutput> output =
                statuses.stream().map(s -> new WorkflowsOutput(s)).collect(Collectors.toList());
            return new WorkflowOutputsResponse(request, output);
          } catch (Exception e) {
            logger.error("Exception encountered when listing workflows", e);
            return new WorkflowOutputsResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleListApplicationVersions(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            var output = conductor.systemDatabase.listApplicationVersions();
            return new ListApplicationVersionsResponse(message, output);
          } catch (Exception e) {
            logger.error("Exception encountered when listing application versions", e);
            return new ListApplicationVersionsResponse(message, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleSetLatestApplicationVersion(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          SetLatestApplicationVersionRequest request = (SetLatestApplicationVersionRequest) message;
          try {
            conductor.dbosExecutor.setLatestApplicationVersion(request.version_name);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error(
                "Exception encountered when setting latest application version to {}",
                request.version_name,
                e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleListSteps(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ListStepsRequest request = (ListStepsRequest) message;
          try {
            List<StepInfo> stepInfoList =
                conductor.dbosExecutor.listWorkflowSteps(request.workflow_id);
            List<ListStepsResponse.Step> steps =
                stepInfoList.stream()
                    .map(i -> new ListStepsResponse.Step(i))
                    .collect(Collectors.toList());
            return new ListStepsResponse(request, steps);
          } catch (Exception e) {
            logger.error("Exception encountered when listing steps {}", request.workflow_id, e);
            return new ListStepsResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleExistPendingWorkflows(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ExistPendingWorkflowsRequest request = (ExistPendingWorkflowsRequest) message;
          try {
            List<GetPendingWorkflowsOutput> pending =
                conductor.systemDatabase.getPendingWorkflows(
                    request.executor_id, request.application_version);
            return new ExistPendingWorkflowsResponse(request, !pending.isEmpty());
          } catch (Exception e) {
            logger.error("Exception encountered when checking for pending workflows", e);
            return new ExistPendingWorkflowsResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleGetWorkflow(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          GetWorkflowRequest request = (GetWorkflowRequest) message;
          try {
            var status = conductor.systemDatabase.getWorkflowStatus(request.workflow_id);
            WorkflowsOutput output = status == null ? null : new WorkflowsOutput(status);
            return new GetWorkflowResponse(request, output);
          } catch (Exception e) {
            logger.error("Exception encountered when getting workflow {}", request.workflow_id, e);
            return new GetWorkflowResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleRetention(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          RetentionRequest request = (RetentionRequest) message;

          try {
            conductor.systemDatabase.garbageCollect(
                request.body.gc_cutoff_epoch_ms, request.body.gc_rows_threshold);
          } catch (Exception e) {
            logger.error("Exception encountered garbage collecting system database", e);
            return new SuccessResponse(request, e);
          }

          try {
            if (request.body.timeout_cutoff_epoch_ms != null) {
              conductor.dbosExecutor.globalTimeout(request.body.timeout_cutoff_epoch_ms);
            }
          } catch (Exception e) {
            logger.error("Exception encountered setting global timeout", e);
            return new SuccessResponse(request, e);
          }

          return new SuccessResponse(request, true);
        });
  }

  static CompletableFuture<BaseResponse> handleGetMetrics(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          GetMetricsRequest request = (GetMetricsRequest) message;

          try {
            if (request.metric_class.equals("workflow_step_count")) {
              var metrics =
                  conductor.systemDatabase.getMetrics(request.startTime(), request.endTime());
              return new GetMetricsResponse(request, metrics);
            } else {
              logger.warn("Unexpected metric class {}", request.metric_class);
              throw new RuntimeException(
                  "Unexpected metric class %s".formatted(request.metric_class));
            }
          } catch (Exception e) {
            return new GetMetricsResponse(request, e);
          }
        });
  }

  private static boolean isImportMessage(CharSequence data) {
    int checkLen = Math.min(data.length(), 200);
    return data.subSequence(0, checkLen).toString().contains("\"import_workflow\"");
  }

  static void streamImportAsync(Conductor conductor, WebSocket ws, Reader pipeReader) {
    CompletableFuture.runAsync(
        () -> {
          long startTime = System.currentTimeMillis();
          logger.info("Starting streaming import workflow");
          String requestId = null;
          try (JsonParser parser = JSONUtil.createParser(pipeReader)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
              throw new IOException("Expected JSON object at start of import message");
            }
            List<ExportedWorkflow> workflows = null;
            TypeReference<List<ExportedWorkflow>> typeRef = new TypeReference<>() {};
            JsonToken token;
            while ((token = parser.nextToken()) != null && token != JsonToken.END_OBJECT) {
              if (token != JsonToken.FIELD_NAME) {
                continue;
              }
              String fieldName = parser.currentName();
              parser.nextToken();
              switch (fieldName) {
                case "request_id":
                  requestId = parser.getValueAsString();
                  break;
                case "serialized_workflow":
                  byte[] decoded = parser.getBinaryValue();
                  try (GZIPInputStream gzip =
                      new GZIPInputStream(new ByteArrayInputStream(decoded))) {
                    workflows = JSONUtil.fromJson(gzip, typeRef);
                  }
                  break;
                default:
                  parser.skipChildren();
                  break;
              }
            }
            if (workflows == null) {
              throw new IOException("Missing serialized_workflow in import message");
            }
            logger.info("Deserialization completed: {} workflows", workflows.size());
            conductor.systemDatabase.importWorkflow(workflows);
            long duration = System.currentTimeMillis() - startTime;
            logger.info(
                "Streaming import completed: {} workflows, duration={}ms",
                workflows.size(),
                duration);
            var req = new ImportWorkflowRequest(requestId, null);
            writeFragmentedResponse(ws, new SuccessResponse(req, true));
          } catch (Exception e) {
            logger.error("Exception during streaming import workflow", e);
            if (requestId != null) {
              try {
                var req = new ImportWorkflowRequest(requestId, null);
                writeFragmentedResponse(ws, new SuccessResponse(req, e));
              } catch (Exception ex) {
                logger.error("Failed to send error response for streaming import", ex);
              }
            }
          }
        });
  }

  static CompletableFuture<BaseResponse> handleImportWorkflow(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ImportWorkflowRequest request = (ImportWorkflowRequest) message;
          long startTime = System.currentTimeMillis();
          logger.info("Starting import workflow");

          try {
            var exportedWorkflows = deserializeExportedWorkflows(request.serialized_workflow);
            logger.info("deserialization completed workflow count={}", exportedWorkflows.size());
            conductor.systemDatabase.importWorkflow(exportedWorkflows);
            long duration = System.currentTimeMillis() - startTime;
            logger.info(
                "Database import completed: {} workflows imported, duration={}ms",
                exportedWorkflows.size(),
                duration);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error("Exception encountered when importing workflow", e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleExportWorkflow(
      Conductor conductor, BaseMessage message, WebSocket ws) {
    return CompletableFuture.supplyAsync(
        () -> {
          ExportWorkflowRequest request = (ExportWorkflowRequest) message;
          long startTime = System.currentTimeMillis();
          logger.info(
              "Starting export workflow: id={}, export_children={}",
              request.workflow_id,
              request.export_children);

          try {
            var workflows =
                conductor.systemDatabase.exportWorkflow(
                    request.workflow_id, request.export_children);

            logger.info(
                "Database export completed: workflow_id={}, {} workflows retrieved",
                request.workflow_id,
                workflows.size());

            streamExportResponse(ws, message, workflows);

            long duration = System.currentTimeMillis() - startTime;
            logger.info(
                "Export workflow streamed: id={}, workflows={}, duration={}ms",
                request.workflow_id,
                workflows.size(),
                duration);

            // null signals to whenComplete that the response was already sent
            return null;
          } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            var children = request.export_children ? "with children" : "";
            logger.error(
                "Exception encountered when exporting workflow {} {} after {}ms",
                request.workflow_id,
                children,
                duration,
                e);
            return new ExportWorkflowResponse(request, e);
          } finally {
            long totalDuration = System.currentTimeMillis() - startTime;
            logger.info(
                "handleExportWorkflow completed: id={}, total_duration={}ms",
                request.workflow_id,
                totalDuration);
          }
        });
  }

  /**
   * Streams an export response directly to the WebSocket without buffering the full payload. The
   * JSON envelope is written manually so the base64+gzip content can be piped straight from Jackson
   * through GZIPOutputStream and Base64 encoding into 128 KB WebSocket frames.
   */
  private static void streamExportResponse(
      WebSocket ws, BaseMessage message, List<ExportedWorkflow> workflows) throws IOException {
    int fragmentSize = 128 * 1024; // 128k
    logger.debug(
        "Starting to stream export response: type={}, id={}", message.type, message.request_id);

    // Build the JSON prefix manually. JSONUtil.toJson(String) produces a properly
    // escaped, double-quoted JSON string value for the request_id field.
    String prefix =
        "{\"type\":\"export_workflow\",\"request_id\":"
            + JSONUtil.toJson(message.request_id)
            + ",\"serialized_workflow\":\"";
    String suffix = "\"}";

    // Wrap fragOut with a non-closing delegate so that base64Out.close() flushes its padding
    // bytes into fragOut without also closing fragOut — we need fragOut open to write the suffix.
    try (FragmentingOutputStream fragOut = new FragmentingOutputStream(ws, fragmentSize)) {
      fragOut.write(prefix.getBytes(StandardCharsets.UTF_8));

      OutputStream nonClosingFragOut =
          new OutputStream() {
            @Override
            public void write(int b) throws IOException {
              fragOut.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
              fragOut.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
              fragOut.flush();
            }

            // close() intentionally does nothing
          };

      // Chain: Jackson -> GZIPOutputStream -> Base64 -> nonClosingFragOut -> fragOut

      try (OutputStream base64Out = Base64.getEncoder().wrap(nonClosingFragOut);
          GZIPOutputStream gzipOut = new GZIPOutputStream(base64Out)) {
        // Note: When the base64Out wrapper closes at the end of the try block, it flushes padding
        // into nonClosingFragOut (and into fragOut), but does NOT close fragOut because of the
        // non-closing delegate.
        JSONUtil.toJson(gzipOut, workflows);
      }

      // Write the closing JSON characters; fragOut.close() sends the final WebSocket frame.
      fragOut.write(suffix.getBytes(StandardCharsets.UTF_8));
    }

    logger.debug(
        "Completed streaming export response: type={}, id={}", message.type, message.request_id);
  }

  static List<ExportedWorkflow> deserializeExportedWorkflows(String serializedWorkflow)
      throws IOException {
    var compressed = Base64.getDecoder().decode(serializedWorkflow);
    try (var gis = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
      var typeRef = new TypeReference<List<ExportedWorkflow>>() {};
      return JSONUtil.fromJson(gis, typeRef);
    }
  }

  // Used by tests to create import payloads and verify export output
  static String serializeExportedWorkflows(List<ExportedWorkflow> workflows) throws IOException {
    var out = new ByteArrayOutputStream();
    try (var gOut = new GZIPOutputStream(out)) {
      JSONUtil.toJson(gOut, workflows);
    }
    return Base64.getEncoder().encodeToString(out.toByteArray());
  }

  static CompletableFuture<BaseResponse> handleAlert(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          AlertRequest request = (AlertRequest) message;
          try {
            conductor.dbosExecutor.fireAlertHandler(
                request.name, request.message, request.metadata);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleListSchedules(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ListSchedulesRequest request = (ListSchedulesRequest) message;
          try {
            List<WorkflowSchedule> schedules =
                conductor.systemDatabase.listSchedules(
                    request.statuses(), request.workflowNames(), request.scheduleNamePrefixes());
            boolean loadContext = request.loadContext();
            List<ScheduleOutput> output =
                schedules.stream().map(s -> ScheduleOutput.from(s, loadContext)).toList();
            return new ListSchedulesResponse(request, output);
          } catch (Exception e) {
            logger.error("Exception encountered when listing schedules", e);
            return new ListSchedulesResponse(request, e.getMessage());
          }
        });
  }

  static CompletableFuture<BaseResponse> handleGetSchedule(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          GetScheduleRequest request = (GetScheduleRequest) message;
          try {
            var schedule = conductor.systemDatabase.findSchedule(request.schedule_name);
            if (schedule.isPresent()) {
              ScheduleOutput output = ScheduleOutput.from(schedule.get(), request.loadContext());
              return new GetScheduleResponse(request, output);
            } else {
              return new GetScheduleResponse(request, (String) null);
            }
          } catch (Exception e) {
            logger.error(
                "Exception encountered when getting schedule {}", request.schedule_name, e);
            return new GetScheduleResponse(request, e.getMessage());
          }
        });
  }

  static CompletableFuture<BaseResponse> handlePauseSchedule(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          PauseScheduleRequest request = (PauseScheduleRequest) message;
          try {
            conductor.systemDatabase.pauseSchedule(request.schedule_name);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error(
                "Exception encountered when pausing schedule {}", request.schedule_name, e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleResumeSchedule(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ResumeScheduleRequest request = (ResumeScheduleRequest) message;
          try {
            conductor.systemDatabase.resumeSchedule(request.schedule_name);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error(
                "Exception encountered when resuming schedule {}", request.schedule_name, e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleBackfillSchedule(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          BackfillScheduleRequest request = (BackfillScheduleRequest) message;
          try {
            var start = Instant.parse(request.start);
            var end = Instant.parse(request.end);
            List<String> workflowIds =
                DBOSExecutor.backfillSchedule(
                    request.schedule_name, start, end, conductor.systemDatabase, null);
            return new BackfillScheduleResponse(request, workflowIds);
          } catch (Exception e) {
            logger.error(
                "Exception encountered when backfilling schedule {}", request.schedule_name, e);
            return new BackfillScheduleResponse(request, e.getMessage());
          }
        });
  }

  static CompletableFuture<BaseResponse> handleTriggerSchedule(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          TriggerScheduleRequest request = (TriggerScheduleRequest) message;
          try {
            String workflowId =
                DBOSExecutor.triggerSchedule(request.schedule_name, conductor.systemDatabase, null);
            return new TriggerScheduleResponse(request, workflowId);
          } catch (Exception e) {
            logger.error(
                "Exception encountered when triggering schedule {}", request.schedule_name, e);
            return new TriggerScheduleResponse(request, e.getMessage(), true);
          }
        });
  }
}
