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
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Conductor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(Conductor.class);
  private static final Map<MessageType, BiFunction<Conductor, BaseMessage, BaseResponse>>
      dispatchMap;

  static {
    Map<MessageType, BiFunction<Conductor, BaseMessage, BaseResponse>> map =
        new java.util.EnumMap<>(MessageType.class);
    map.put(MessageType.EXECUTOR_INFO, Conductor::handleExecutorInfo);
    map.put(MessageType.RECOVERY, Conductor::handleRecovery);
    map.put(MessageType.CANCEL, Conductor::handleCancel);
    map.put(MessageType.RESUME, Conductor::handleResume);
    map.put(MessageType.RESTART, Conductor::handleRestart);
    map.put(MessageType.FORK_WORKFLOW, Conductor::handleFork);
    map.put(MessageType.LIST_WORKFLOWS, Conductor::handleListWorkflows);
    map.put(MessageType.LIST_QUEUED_WORKFLOWS, Conductor::handleListQueuedWorkflows);
    map.put(MessageType.LIST_STEPS, Conductor::handleListSteps);
    map.put(MessageType.EXIST_PENDING_WORKFLOWS, Conductor::handleExistPendingWorkflows);
    map.put(MessageType.GET_WORKFLOW, Conductor::handleGetWorkflow);
    map.put(MessageType.RETENTION, Conductor::handleRetention);
    map.put(MessageType.GET_METRICS, Conductor::handleGetMetrics);
    dispatchMap = Collections.unmodifiableMap(map);
  }

  private final int pingPeriodMs;
  private final int pingTimeoutMs;
  private final int reconnectDelayMs;
  private final int connectTimeoutMs;

  private final String url;
  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private Channel channel;
  private EventLoopGroup group;
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
    this.connectTimeoutMs = builder.connectTimeoutMs;
  }

  public static class Builder {
    private SystemDatabase systemDatabase;
    private DBOSExecutor dbosExecutor;
    private String conductorKey;
    private String domain;
    private int pingPeriodMs = 20000;
    private int pingTimeoutMs = 15000;
    private int reconnectDelayMs = 1000;
    private int connectTimeoutMs = 5000;

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

    Builder connectTimeoutMs(int connectTimeoutMs) {
      this.connectTimeoutMs = connectTimeoutMs;
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
    dispatchLoop();
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

      if (channel != null && channel.isActive()) {
        channel.writeAndFlush(new CloseWebSocketFrame());
        channel.close();
        channel = null;
      }

      if (group != null) {
        group.shutdownGracefully();
        group = null;
      }
    }
  }

  void setPingInterval(Channel channel) {
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
                // Check for null in case channel connects before channel variable is assigned
                if (channel == null) {
                  logger.debug("channel null, NOT sending ping to conductor");
                  return;
                }

                if (!channel.isActive()) {
                  logger.debug("channel closed, NOT sending ping to conductor");
                  return;
                }

                logger.debug("Sending ping to conductor");
                channel
                    .writeAndFlush(new PingWebSocketFrame(Unpooled.buffer(0)))
                    .addListener(
                        future -> {
                          if (!future.isSuccess()) {
                            logger.error("Failed to send ping to conductor", future.cause());
                            resetWebSocket();
                          }
                        });

                pingTimeout =
                    scheduler.schedule(
                        () -> {
                          if (!isShutdown.get()) {
                            logger.warn("pingTimeout: Connection to conductor lost. Reconnecting.");
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
    if (pingInterval != null) {
      pingInterval.cancel(false);
      pingInterval = null;
    }

    if (pingTimeout != null) {
      pingTimeout.cancel(false);
      pingTimeout = null;
    }

    if (channel != null) {
      channel.close();
      channel = null;
    }

    if (group != null) {
      group.shutdownGracefully();
      group = null;
    }

    if (isShutdown.get()) {
      return;
    }

    if (reconnectTimeout == null) {
      reconnectTimeout =
          scheduler.schedule(
              () -> {
                reconnectTimeout = null;
                dispatchLoop();
              },
              reconnectDelayMs,
              TimeUnit.MILLISECONDS);
    }
  }

  void dispatchLoop() {
    if (channel != null) {
      logger.warn("Conductor websocket already exists");
      return;
    }

    if (isShutdown.get()) {
      logger.debug("Not starting dispatch loop as conductor is shutting down");
      return;
    }

    // Log environment variables that might affect authentication
    logger.debug("===== Environment Check =====");
    logger.debug("DBOS_DOMAIN: {}", System.getenv("DBOS_DOMAIN"));
    logger.debug("DBOS__CLOUD: {}", System.getenv("DBOS__CLOUD"));
    logger.debug("DBOS__APPID: {}", System.getenv("DBOS__APPID"));
    logger.debug("DBOS__VMID: {}", System.getenv("DBOS__VMID"));
    logger.debug("DBOS__APPVERSION: {}", System.getenv("DBOS__APPVERSION"));

    // Check for any DBOS or auth-related environment variables
    System.getenv().entrySet().stream()
        .filter(
            e ->
                e.getKey().toUpperCase().contains("DBOS")
                    || e.getKey().toUpperCase().contains("AUTH")
                    || e.getKey().toUpperCase().contains("TOKEN"))
        .forEach(
            e ->
                logger.debug(
                    "Auth env: {}={}",
                    e.getKey(),
                    e.getKey().toLowerCase().contains("token")
                            || e.getKey().toLowerCase().contains("password")
                        ? "[REDACTED]"
                        : e.getValue()));
    logger.debug("===== Environment Check Complete =====");

    try {
      logger.debug("Connecting to conductor at {}", url);

      // Log the URL being used
      URI wsUri = URI.create(url);
      logger.debug("WebSocket URI: {}", wsUri);
      logger.debug(
          "URI scheme: {}, host: {}, port: {}, path: {}",
          wsUri.getScheme(),
          wsUri.getHost(),
          wsUri.getPort(),
          wsUri.getPath());

      // Create event loop group
      group = new NioEventLoopGroup();

      // Create SSL context for WSS
      SslContext sslCtx =
          SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

      // Create headers that EXACTLY match the working HttpClient implementation
      DefaultHttpHeaders headers = new DefaultHttpHeaders();
      headers.add("User-Agent", "DBOS-Java-Debug/" + System.getProperty("java.version"));
      headers.add("X-Debug-Test", "test-value");

      logger.debug("WebSocket builder timeout: {}ms", connectTimeoutMs);
      logger.debug("Adding debug headers to WebSocket request");

      // Set up the WebSocket handshaker with the exact same parameters
      int port =
          wsUri.getPort() == -1 ? (wsUri.getScheme().equals("wss") ? 443 : 80) : wsUri.getPort();
      WebSocketClientHandshaker handshaker =
          WebSocketClientHandshakerFactory.newHandshaker(
              wsUri,
              WebSocketVersion.V13,
              null, // No subprotocol
              true, // Allow extensions
              headers,
              65536); // Max frame payload length

      // Bootstrap the client
      Bootstrap bootstrap = new Bootstrap();
      final Conductor conductor = this;

      ChannelFuture future =
          bootstrap
              .group(group)
              .channel(NioSocketChannel.class)
              .handler(
                  new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                      ChannelPipeline pipeline = ch.pipeline();
                      pipeline.addLast("ssl", sslCtx.newHandler(ch.alloc(), wsUri.getHost(), port));
                      pipeline.addLast("http-codec", new HttpClientCodec());
                      pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                      pipeline.addLast(
                          "websocket-handler",
                          new SimpleChannelInboundHandler<Object>() {

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                              handshaker.handshake(ctx.channel());
                              super.channelActive(ctx);
                            }

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg)
                                throws Exception {
                              Channel ch = ctx.channel();

                              if (!handshaker.isHandshakeComplete()) {
                                try {
                                  handshaker.finishHandshake(
                                      ch, (io.netty.handler.codec.http.FullHttpResponse) msg);
                                  logger.info("===== SUCCESSFUL WebSocket Connection =====");
                                  logger.info("Connected to DBOS conductor at: {}", url);
                                  logger.info("Channel state: {}", ch);
                                  logger.debug("Channel class: {}", ch.getClass().getName());

                                  setPingInterval(ch);
                                  logger.info("===== WebSocket setup complete =====");
                                  return;
                                } catch (Exception e) {
                                  logger.error("WebSocket handshake failed:", e);
                                  return;
                                }
                              }

                              if (msg instanceof TextWebSocketFrame) {
                                TextWebSocketFrame textFrame = (TextWebSocketFrame) msg;
                                String text = textFrame.text();

                                BaseMessage request;
                                try {
                                  request = JSONUtil.fromJson(text, BaseMessage.class);
                                } catch (Exception e) {
                                  logger.error("Conductor JSON Parsing error", e);
                                  return;
                                }

                                String responseText;
                                try {
                                  BaseResponse response = getResponse(request);
                                  responseText = JSONUtil.toJson(response);
                                } catch (Exception e) {
                                  logger.error("Conductor Response error", e);
                                  return;
                                }

                                ch.writeAndFlush(new TextWebSocketFrame(responseText));
                              } else if (msg instanceof PongWebSocketFrame) {
                                logger.debug("Received pong from conductor");
                                if (pingTimeout != null) {
                                  pingTimeout.cancel(false);
                                  pingTimeout = null;
                                }
                              } else if (msg instanceof CloseWebSocketFrame) {
                                if (isShutdown.get()) {
                                  logger.debug("Shutdown Conductor connection");
                                } else if (reconnectTimeout == null) {
                                  logger.warn(
                                      "onClose: Connection to conductor lost. Reconnecting");
                                  resetWebSocket();
                                }
                                ch.close();
                              }
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                                throws Exception {
                              logger.error("===== WebSocket Connection Error =====");
                              logger.error("Error type: {}", cause.getClass().getName());
                              logger.error("Error message: {}", cause.getMessage());
                              logger.error("URL: {}", url);
                              logger.error("Channel state: {}", ctx.channel());
                              logger.error("Full error:", cause);
                              logger.error("===== End WebSocket Error =====");
                              resetWebSocket();
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx)
                                throws Exception {
                              if (isShutdown.get()) {
                                logger.debug("Shutdown Conductor connection");
                              } else if (reconnectTimeout == null) {
                                logger.warn(
                                    "channelInactive: Connection to conductor lost. Reconnecting");
                                resetWebSocket();
                              }
                              super.channelInactive(ctx);
                            }
                          });
                    }
                  })
              .connect(wsUri.getHost(), port);

      channel = future.sync().channel();

      logger.info("===== WebSocket buildAsync completed =====");
      logger.info("WebSocket created successfully: {}", channel != null);
      if (channel != null) {
        logger.info("WebSocket class: {}", channel.getClass().getName());
        logger.info("WebSocket toString: {}", channel);
      }
      logger.info("===== WebSocket creation complete =====");

    } catch (Exception e) {
      logger.error("===== Error in conductor loop =====");
      logger.error("Exception type: {}", e.getClass().getName());
      logger.error("Exception message: {}", e.getMessage());
      logger.error("Full exception:", e);
      logger.error("===== End conductor loop error =====");
      resetWebSocket();
    }
  }

  BaseResponse getResponse(BaseMessage message) {
    logger.debug("getResponse {}", message.type);
    MessageType messageType = MessageType.fromValue(message.type);
    BiFunction<Conductor, BaseMessage, BaseResponse> func = dispatchMap.get(messageType);
    if (func != null) {
      return func.apply(this, message);
    } else {
      logger.warn("Conductor unknown message type {}", message.type);
      return new BaseResponse(message.type, message.request_id, "Unknown message type");
    }
  }

  static BaseResponse handleExecutorInfo(Conductor conductor, BaseMessage message) {
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
  }

  static BaseResponse handleRecovery(Conductor conductor, BaseMessage message) {
    RecoveryRequest request = (RecoveryRequest) message;
    try {
      conductor.dbosExecutor.recoverPendingWorkflows(request.executor_ids);
      return new SuccessResponse(request, true);
    } catch (Exception e) {
      logger.error("Exception encountered when recovering pending workflows", e);
      return new SuccessResponse(request, e);
    }
  }

  static BaseResponse handleCancel(Conductor conductor, BaseMessage message) {
    CancelRequest request = (CancelRequest) message;
    try {
      conductor.dbosExecutor.cancelWorkflow(request.workflow_id);
      return new SuccessResponse(request, true);
    } catch (Exception e) {
      logger.error("Exception encountered when cancelling workflow {}", request.workflow_id, e);
      return new SuccessResponse(request, e);
    }
  }

  static BaseResponse handleResume(Conductor conductor, BaseMessage message) {
    ResumeRequest request = (ResumeRequest) message;
    try {
      conductor.dbosExecutor.resumeWorkflow(request.workflow_id);
      return new SuccessResponse(request, true);
    } catch (Exception e) {
      logger.error("Exception encountered when resuming workflow {}", request.workflow_id, e);
      return new SuccessResponse(request, e);
    }
  }

  static BaseResponse handleRestart(Conductor conductor, BaseMessage message) {
    RestartRequest request = (RestartRequest) message;
    try {
      ForkOptions options = new ForkOptions();
      conductor.dbosExecutor.forkWorkflow(request.workflow_id, 0, options);
      return new SuccessResponse(request, true);
    } catch (Exception e) {
      logger.error("Exception encountered when restarting workflow {}", request.workflow_id, e);
      return new SuccessResponse(request, e);
    }
  }

  static BaseResponse handleFork(Conductor conductor, BaseMessage message) {
    ForkWorkflowRequest request = (ForkWorkflowRequest) message;
    if (request.body.workflow_id == null || request.body.start_step == null) {
      return new ForkWorkflowResponse(request, null, "Invalid Fork Workflow Request");
    }
    try {
      var options =
          new ForkOptions(request.body.new_workflow_id, request.body.application_version, null);
      WorkflowHandle<?, ?> handle =
          conductor.dbosExecutor.forkWorkflow(
              request.body.workflow_id, request.body.start_step, options);
      return new ForkWorkflowResponse(request, handle.workflowId());
    } catch (Exception e) {
      logger.error("Exception encountered when forking workflow {}", request, e);
      return new ForkWorkflowResponse(request, e);
    }
  }

  static BaseResponse handleListWorkflows(Conductor conductor, BaseMessage message) {
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
  }

  static BaseResponse handleListQueuedWorkflows(Conductor conductor, BaseMessage message) {
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
  }

  static BaseResponse handleListSteps(Conductor conductor, BaseMessage message) {
    ListStepsRequest request = (ListStepsRequest) message;
    try {
      List<StepInfo> stepInfoList = conductor.dbosExecutor.listWorkflowSteps(request.workflow_id);
      List<ListStepsResponse.Step> steps =
          stepInfoList.stream()
              .map(i -> new ListStepsResponse.Step(i))
              .collect(Collectors.toList());
      return new ListStepsResponse(request, steps);
    } catch (Exception e) {
      logger.error("Exception encountered when listing steps {}", request.workflow_id, e);
      return new ListStepsResponse(request, e);
    }
  }

  static BaseResponse handleExistPendingWorkflows(Conductor conductor, BaseMessage message) {
    ExistPendingWorkflowsRequest request = (ExistPendingWorkflowsRequest) message;
    try {
      List<GetPendingWorkflowsOutput> pending =
          conductor.systemDatabase.getPendingWorkflows(
              request.executor_id, request.application_version);
      return new ExistPendingWorkflowsResponse(request, pending.size() > 0);
    } catch (Exception e) {
      logger.error("Exception encountered when checking for pending workflows", e);
      return new ExistPendingWorkflowsResponse(request, e);
    }
  }

  static BaseResponse handleGetWorkflow(Conductor conductor, BaseMessage message) {
    GetWorkflowRequest request = (GetWorkflowRequest) message;
    try {
      var status = conductor.systemDatabase.getWorkflowStatus(request.workflow_id);
      WorkflowsOutput output = status == null ? null : new WorkflowsOutput(status);
      return new GetWorkflowResponse(request, output);
    } catch (Exception e) {
      logger.error("Exception encountered when getting workflow {}", request.workflow_id, e);
      return new GetWorkflowResponse(request, e);
    }
  }

  static BaseResponse handleRetention(Conductor conductor, BaseMessage message) {
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
  }

  static BaseResponse handleGetMetrics(Conductor conductor, BaseMessage message) {
    GetMetricsRequest request = (GetMetricsRequest) message;

    try {
      if (request.metric_class.equals("workflow_step_count")) {
        var metrics = conductor.systemDatabase.getMetrics(request.startTime(), request.endTime());
        return new GetMetricsResponse(request, metrics);
      } else {
        logger.warn("Unexpected metric class {}", request.metric_class);
        throw new RuntimeException("Unexpected metric class %s".formatted(request.metric_class));
      }
    } catch (Exception e) {
      return new GetMetricsResponse(request, e);
    }
  }

  static CompletableFuture<BaseResponse> handleExportWorkflow(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ExportWorkflowRequest request = (ExportWorkflowRequest) message;
          long startTime = System.currentTimeMillis();
          try {
            var workflows =
                conductor.systemDatabase.exportWorkflow(
                    request.workflow_id, request.export_children);
            logger.info("Queried database workflow count={}", workflows.size());

            var serializedWorkflow = serializeExportedWorkflows(workflows);

            long duration = System.currentTimeMillis() - startTime;
            logger.info(
                "Export workflow completed: id={}, workflows={}, serialized_size={} bytes, duration={}ms",
                request.workflow_id,
                workflows.size(),
                serializedWorkflow.length(),
                duration);

            return new ExportWorkflowResponse(message, serializedWorkflow);
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
          }
        });
  }

  static CompletableFuture<BaseResponse> handleImportWorkflow(
      Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ImportWorkflowRequest request = (ImportWorkflowRequest) message;
          long startTime = System.currentTimeMillis();
          try {
            logger.info("Starting workflow import");
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

  static List<ExportedWorkflow> deserializeExportedWorkflows(String serializedWorkflow)
      throws IOException {
    var compressed = Base64.getDecoder().decode(serializedWorkflow);
    try (var gis = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
      var typeRef = new TypeReference<List<ExportedWorkflow>>() {};
      return JSONUtil.fromJson(gis, typeRef);
    }
  }

  static String serializeExportedWorkflows(List<ExportedWorkflow> workflows) throws IOException {
    var out = new ByteArrayOutputStream();
    try (var gOut = new GZIPOutputStream(out)) {
      JSONUtil.toJson(gOut, workflows);
    }

    return Base64.getEncoder().encodeToString(out.toByteArray());
  }
}
