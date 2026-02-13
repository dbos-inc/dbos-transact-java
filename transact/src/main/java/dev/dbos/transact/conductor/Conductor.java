package dev.dbos.transact.conductor;

import dev.dbos.transact.conductor.protocol.AlertRequest;
import dev.dbos.transact.conductor.protocol.BaseMessage;
import dev.dbos.transact.conductor.protocol.BaseResponse;
import dev.dbos.transact.conductor.protocol.CancelRequest;
import dev.dbos.transact.conductor.protocol.DeleteRequest;
import dev.dbos.transact.conductor.protocol.ExecutorInfoResponse;
import dev.dbos.transact.conductor.protocol.ExistPendingWorkflowsRequest;
import dev.dbos.transact.conductor.protocol.ExistPendingWorkflowsResponse;
import dev.dbos.transact.conductor.protocol.ExportWorkflowRequest;
import dev.dbos.transact.conductor.protocol.ExportWorkflowResponse;
import dev.dbos.transact.conductor.protocol.ForkWorkflowRequest;
import dev.dbos.transact.conductor.protocol.ForkWorkflowResponse;
import dev.dbos.transact.conductor.protocol.GetMetricsRequest;
import dev.dbos.transact.conductor.protocol.GetMetricsResponse;
import dev.dbos.transact.conductor.protocol.GetWorkflowRequest;
import dev.dbos.transact.conductor.protocol.GetWorkflowResponse;
import dev.dbos.transact.conductor.protocol.ImportWorkflowRequest;
import dev.dbos.transact.conductor.protocol.ListQueuedWorkflowsRequest;
import dev.dbos.transact.conductor.protocol.ListStepsRequest;
import dev.dbos.transact.conductor.protocol.ListStepsResponse;
import dev.dbos.transact.conductor.protocol.ListWorkflowsRequest;
import dev.dbos.transact.conductor.protocol.MessageType;
import dev.dbos.transact.conductor.protocol.RecoveryRequest;
import dev.dbos.transact.conductor.protocol.RestartRequest;
import dev.dbos.transact.conductor.protocol.ResumeRequest;
import dev.dbos.transact.conductor.protocol.RetentionRequest;
import dev.dbos.transact.conductor.protocol.SuccessResponse;
import dev.dbos.transact.conductor.protocol.WorkflowOutputsResponse;
import dev.dbos.transact.conductor.protocol.WorkflowsOutput;
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
import java.io.InputStream;
import java.io.OutputStream;
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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Conductor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(Conductor.class);
  private static final Map<
          MessageType, BiFunction<Conductor, BaseMessage, CompletableFuture<BaseResponse>>>
      dispatchMap;

  static {
    Map<MessageType, BiFunction<Conductor, BaseMessage, CompletableFuture<BaseResponse>>> map =
        new java.util.EnumMap<>(MessageType.class);
    map.put(MessageType.ALERT, Conductor::handleAlert);
    map.put(MessageType.CANCEL, Conductor::handleCancel);
    map.put(MessageType.DELETE, Conductor::handleDelete);
    map.put(MessageType.EXECUTOR_INFO, Conductor::handleExecutorInfo);
    map.put(MessageType.EXIST_PENDING_WORKFLOWS, Conductor::handleExistPendingWorkflows);
    map.put(MessageType.EXPORT_WORKFLOW, Conductor::handleExportWorkflow);
    map.put(MessageType.FORK_WORKFLOW, Conductor::handleFork);
    map.put(MessageType.GET_METRICS, Conductor::handleGetMetrics);
    map.put(MessageType.GET_WORKFLOW, Conductor::handleGetWorkflow);
    map.put(MessageType.IMPORT_WORKFLOW, Conductor::handleImportWorkflow);
    map.put(MessageType.LIST_QUEUED_WORKFLOWS, Conductor::handleListQueuedWorkflows);
    map.put(MessageType.LIST_STEPS, Conductor::handleListSteps);
    map.put(MessageType.LIST_WORKFLOWS, Conductor::handleListWorkflows);
    map.put(MessageType.RECOVERY, Conductor::handleRecovery);
    map.put(MessageType.RESTART, Conductor::handleRestart);
    map.put(MessageType.RESUME, Conductor::handleResume);
    map.put(MessageType.RETENTION, Conductor::handleRetention);

    dispatchMap = Collections.unmodifiableMap(map);
  }

  private final int pingPeriodMs;
  private final int pingTimeoutMs;
  private final int reconnectDelayMs;

  private final String url;
  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private Channel channel;
  private EventLoopGroup group;
  private NettyWebSocketHandler handler;
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
  }

  private class NettyWebSocketHandler extends SimpleChannelInboundHandler<Object> {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
        logger.info("Successfully established websocket connection to DBOS conductor at {}", url);
        setPingInterval(ctx.channel());
      } else if (evt
          == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_TIMEOUT) {
        logger.error("Websocket handshake timeout with conductor at {}", url);
      }
      super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof PingWebSocketFrame ping) {
        logger.debug("Received ping from conductor");
        ctx.channel().writeAndFlush(new PongWebSocketFrame(ping.content().retain()));
      } else if (msg instanceof PongWebSocketFrame) {
        logger.debug("Received pong from conductor");
        if (pingTimeout != null) {
          pingTimeout.cancel(false);
          pingTimeout = null;
          logger.debug("Cancelled ping timeout - connection is healthy");
        } else {
          logger.debug("Received pong but no ping timeout was active");
        }
      } else if (msg instanceof CloseWebSocketFrame closeFrame) {
        logger.warn(
            "Received close frame from conductor: status={}, reason='{}'",
            closeFrame.statusCode(),
            closeFrame.reasonText());
        if (isShutdown.get()) {
          logger.debug("Shutdown Conductor connection");
        } else if (reconnectTimeout == null) {
          logger.warn("onClose: Connection to conductor lost. Reconnecting");
          resetWebSocket();
        }
      } else if (msg instanceof ByteBuf content) {
        int messageSize = content.readableBytes();
        logger.debug("Received {} bytes from Conductor {}", messageSize, msg.getClass().getName());

        BaseMessage request;
        try (InputStream is = new ByteBufInputStream(content)) {
          request = JSONUtil.fromJson(is, BaseMessage.class);
        } catch (Exception e) {
          logger.error("Conductor JSON Parsing error for {} byte message", messageSize, e);
          return;
        }

        try {
          long startTime = System.currentTimeMillis();
          logger.info(
              "Processing conductor request: type={}, id={}", request.type, request.request_id);

          getResponseAsync(request)
              .whenComplete(
                  (response, throwable) -> {
                    try {
                      long processingTime = System.currentTimeMillis() - startTime;
                      if (throwable != null) {
                        logger.error(
                            "Error processing request: type={}, id={}, duration={}ms",
                            request.type,
                            request.request_id,
                            processingTime,
                            throwable);

                        // Create an error response
                        BaseResponse errorResponse =
                            new BaseResponse(
                                request.type, request.request_id, throwable.getMessage());
                        writeFragmentedResponse(ctx, errorResponse);
                      } else {
                        logger.info(
                            "Completed processing request: type={}, id={}, duration={}ms",
                            request.type,
                            request.request_id,
                            processingTime);
                        writeFragmentedResponse(ctx, response);
                      }
                    } catch (Exception e) {
                      logger.error(
                          "Error writing response for request type={}, id={}",
                          request.type,
                          request.request_id,
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
      }
    }

    private static void writeFragmentedResponse(ChannelHandlerContext ctx, BaseResponse response)
        throws Exception {
      int fragmentSize = 128 * 1024; // 128k
      logger.debug(
          "Starting to write fragmented response: type={}, id={}",
          response.type,
          response.request_id);
      try (OutputStream out = new FragmentingOutputStream(ctx, fragmentSize)) {
        JSONUtil.toJsonStream(response, out);
      }
      logger.debug(
          "Completed writing fragmented response: type={}, id={}",
          response.type,
          response.request_id);
    }

    private static class FragmentingOutputStream extends OutputStream {
      private final ChannelHandlerContext ctx;
      private final int fragmentSize;
      private ByteBuf currentBuffer;
      private boolean firstFrame = true;
      private boolean closed = false;

      public FragmentingOutputStream(ChannelHandlerContext ctx, int fragmentSize) {
        this.ctx = ctx;
        this.fragmentSize = fragmentSize;
        this.currentBuffer = ctx.alloc().buffer(fragmentSize);
        logger.debug("Created FragmentingOutputStream with fragment size: {}", fragmentSize);
      }

      @Override
      public void write(int b) throws IOException {
        currentBuffer.writeByte(b);
        if (currentBuffer.readableBytes() == fragmentSize) {
          flushBuffer(false);
        }
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        while (len > 0) {
          int toCopy = Math.min(len, fragmentSize - currentBuffer.readableBytes());
          currentBuffer.writeBytes(b, off, toCopy);
          off += toCopy;
          len -= toCopy;
          if (currentBuffer.readableBytes() == fragmentSize) {
            flushBuffer(false);
          }
        }
      }

      private void flushBuffer(boolean last) {
        if (currentBuffer.readableBytes() == 0 && !last) {
          return;
        }

        int frameSize = currentBuffer.readableBytes();
        WebSocketFrame frame;
        if (firstFrame) {
          frame = new TextWebSocketFrame(last, 0, currentBuffer);
          firstFrame = false;
        } else {
          frame = new ContinuationWebSocketFrame(last, 0, currentBuffer);
        }

        try {
          ctx.channel()
              .writeAndFlush(frame)
              .addListener(
                  future -> {
                    if (!future.isSuccess()) {
                      logger.error(
                          "Failed to send websocket frame: {} bytes", frameSize, future.cause());
                    }
                  });
        } catch (Exception e) {
          logger.error("Exception while sending websocket frame: {} bytes", frameSize, e);
          throw e;
        }

        if (!last) {
          currentBuffer = ctx.alloc().buffer(fragmentSize);
        } else {
          currentBuffer = null;
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      logger.warn(
          "Unexpected exception in websocket connection to conductor. Channel active: {}, writable: {}",
          ctx.channel().isActive(),
          ctx.channel().isWritable(),
          cause);
      resetWebSocket();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      logger.warn(
          "Websocket channel became inactive. Shutdown: {}, reconnect pending: {}",
          isShutdown.get(),
          reconnectTimeout != null);
      if (!isShutdown.get() && reconnectTimeout == null) {
        logger.warn("Channel inactive: Connection to conductor lost. Reconnecting");
        resetWebSocket();
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

      if (channel != null) {
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
                if (channel == null || !channel.isActive()) {
                  logger.debug("channel not active, NOT sending ping to conductor");
                  return;
                }

                logger.debug("Sending ping to conductor (timeout in {}ms)", pingTimeoutMs);
                channel
                    .writeAndFlush(new PingWebSocketFrame())
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
                            logger.error(
                                "Ping timeout after {}ms - no pong received from conductor. Connection lost, reconnecting.",
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
        "Resetting websocket connection. Channel active: {}",
        channel != null ? channel.isActive() : "null");

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
    if (channel != null) {
      logger.warn("Conductor channel already exists");
      return;
    }

    if (isShutdown.get()) {
      logger.debug("Not connecting web socket as conductor is shutting down");
      return;
    }

    try {
      logger.debug("Connecting to conductor at {}", url);
      URI uri = new URI(url);
      String scheme = uri.getScheme() == null ? "ws" : uri.getScheme();
      final String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
      final int port;
      if (uri.getPort() == -1) {
        if ("ws".equalsIgnoreCase(scheme)) {
          port = 80;
        } else if ("wss".equalsIgnoreCase(scheme)) {
          port = 443;
        } else {
          port = -1;
        }
      } else {
        port = uri.getPort();
      }

      if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
        logger.error("Only WS(S) is supported.");
        return;
      }

      final boolean ssl = "wss".equalsIgnoreCase(scheme);
      final SslContext sslCtx;
      if (ssl) {
        sslCtx =
            SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
      } else {
        sslCtx = null;
      }

      group = new NioEventLoopGroup();
      handler = new NettyWebSocketHandler();

      Bootstrap b = new Bootstrap();
      b.group(group)
          .channel(NioSocketChannel.class)
          .handler(
              new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                  var p = ch.pipeline();
                  if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                  }
                  p.addLast(
                      new HttpClientCodec(),
                      new HttpObjectAggregator(256 * 1024 * 1024), // 256MB max message size
                      new WebSocketClientProtocolHandler(
                          WebSocketClientProtocolConfig.newBuilder()
                              .webSocketUri(uri)
                              .version(WebSocketVersion.V13)
                              .subprotocol(null)
                              .allowExtensions(false)
                              .customHeaders(EmptyHttpHeaders.INSTANCE)
                              .dropPongFrames(false)
                              .handleCloseFrames(false)
                              .generateOriginHeader(false)
                              .maxFramePayloadLength(256 * 1024 * 1024)
                              .build()),
                      new MessageToMessageDecoder<WebSocketFrame>() {
                        @Override
                        protected void decode(
                            ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) {
                          if (frame instanceof TextWebSocketFrame
                              || frame instanceof ContinuationWebSocketFrame) {
                            out.add(frame.content().retain());
                          } else {
                            out.add(frame.retain());
                          }
                        }
                      },
                      new JsonObjectDecoder(256 * 1024 * 1024) {
                        {
                          setCumulator(COMPOSITE_CUMULATOR);
                        }
                      },
                      handler);
                }
              });

      ChannelFuture future = b.connect(host, port);
      channel = future.channel();
      future.addListener(
          f -> {
            if (f.isSuccess()) {
              logger.info("Successfully connected to conductor at {}:{}", host, port);
            } else {
              logger.warn(
                  "Failed to connect to conductor at {}:{}. Reconnecting", host, port, f.cause());
              resetWebSocket();
            }
          });

    } catch (Exception e) {
      logger.warn("Error in conductor loop. Reconnecting", e);
      resetWebSocket();
    }
  }

  CompletableFuture<BaseResponse> getResponseAsync(BaseMessage message) {
    logger.debug("getResponseAsync {}", message.type);
    MessageType messageType = MessageType.fromValue(message.type);
    BiFunction<Conductor, BaseMessage, CompletableFuture<BaseResponse>> func =
        dispatchMap.get(messageType);
    if (func != null) {
      return func.apply(this, message);
    } else {
      logger.warn("Conductor unknown message type {}", message.type);
      return CompletableFuture.completedFuture(
          new BaseResponse(message.type, message.request_id, "Unknown message type"));
    }
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
          try {
            conductor.dbosExecutor.cancelWorkflow(request.workflow_id);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error(
                "Exception encountered when cancelling workflow {}", request.workflow_id, e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleDelete(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          DeleteRequest request = (DeleteRequest) message;
          try {
            conductor.dbosExecutor.deleteWorkflow(request.workflow_id, request.delete_children);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error("Exception encountered when deleting workflow {}", request.workflow_id, e);
            return new SuccessResponse(request, e);
          }
        });
  }

  static CompletableFuture<BaseResponse> handleResume(Conductor conductor, BaseMessage message) {
    return CompletableFuture.supplyAsync(
        () -> {
          ResumeRequest request = (ResumeRequest) message;
          try {
            conductor.dbosExecutor.resumeWorkflow(request.workflow_id);
            return new SuccessResponse(request, true);
          } catch (Exception e) {
            logger.error("Exception encountered when resuming workflow {}", request.workflow_id, e);
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
            return new ExistPendingWorkflowsResponse(request, pending.size() > 0);
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
      Conductor conductor, BaseMessage message) {
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
          } finally {
            long totalDuration = System.currentTimeMillis() - startTime;
            logger.info(
                "handleExportWorkflow completed: id={}, total_duration={}ms",
                request.workflow_id,
                totalDuration);
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
}
