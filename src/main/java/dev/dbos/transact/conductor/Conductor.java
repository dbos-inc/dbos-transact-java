package dev.dbos.transact.conductor;

import dev.dbos.transact.conductor.protocol.*;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Conductor implements AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(Conductor.class);
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static final Map<MessageType, BiFunction<Conductor, BaseMessage, BaseResponse>> dispatchMap;

    static {
        Map<MessageType, BiFunction<Conductor, BaseMessage, BaseResponse>> map = new java.util.EnumMap<>(
                MessageType.class);
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
        dispatchMap = Collections.unmodifiableMap(map);
    }

    private final int pingPeriodMs;
    private final int pingTimeoutMs;
    private final int reconnectDelayMs;
    private final int connectTimeoutMs;

    private final String url;
    private final SystemDatabase systemDatabase;
    private final DBOSExecutor dbosExecutor;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    private WebSocket webSocket;
    private ScheduledFuture<?> pingInterval;
    private ScheduledFuture<?> pingTimeout;
    private ScheduledFuture<?> reconnectTimeout;

    private Conductor(Builder builder) {
        Objects.requireNonNull(builder.systemDatabase);
        Objects.requireNonNull(builder.dbosExecutor);
        Objects.requireNonNull(builder.conductorKey);

        this.systemDatabase = builder.systemDatabase;
        this.dbosExecutor = builder.dbosExecutor;

        String appName = dbosExecutor.getAppName();
        Objects.requireNonNull(appName, "App Name must not be null to use Conductor");

        String domain = builder.domain;
        if (domain == null) {
            String dbosDomain = System.getenv("DBOS_DOMAIN");
            if (dbosDomain == null || dbosDomain.trim().isEmpty()) {
                domain = "wss://cloud.dbos.dev";
            } else {
                domain = "wss://" + dbosDomain.trim();
            }
        }

        this.url = domain + "/conductor/v1alpha1/websocket/" + appName + "/" + builder.conductorKey;

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

        public Builder(SystemDatabase s, DBOSExecutor e, String key) {
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
    public void close() throws Exception {
        this.stop();
    }

    public void start() {
        dispatchLoop();
    }

    public void stop() {
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

            if (webSocket != null) {
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "");
                webSocket = null;
            }
        }
    }

    void setPingInterval() {
        logger.info("setPingInterval");

        if (pingInterval != null) {
            pingInterval.cancel(false);
        }
        pingInterval = scheduler.scheduleAtFixedRate(() -> {
            try {
                logger.info("setPingInterval::scheduleAtFixedRate");
                // Note, checking for null because websocket can connect before websocket
                // variable is assigned
                if (webSocket != null && !webSocket.isOutputClosed()) {
                    logger.info("Sending ping to conductor");

                    webSocket.sendPing(ByteBuffer.allocate(0))
                            .exceptionally(ex -> {
                                logger.error("Failed to send ping to conductor", ex);
                                resetWebSocket();
                                return null;
                            });

                    pingTimeout = scheduler.schedule(() -> {
                        if (!isShutdown.get()) {
                            logger.warn("pingTimeout: Connection to conductor lost. Reconnecting.");
                            resetWebSocket();
                        }
                    }, pingTimeoutMs, TimeUnit.MILLISECONDS);
                } else {
                    logger.info("NOT Sending ping to conductor");
                }
            } catch (Exception e) {
                logger.error("setPingInterval::scheduleAtFixedRate catch", e);

            }
        }, 0, pingPeriodMs, TimeUnit.MILLISECONDS);
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

        if (webSocket != null) {
            webSocket.abort();
            webSocket = null;
        }

        if (isShutdown.get()) {
            return;
        }

        if (reconnectTimeout == null) {
            reconnectTimeout = scheduler.schedule(() -> {
                reconnectTimeout = null;
                dispatchLoop();
            }, reconnectDelayMs, TimeUnit.MILLISECONDS);
        }
    }

    void dispatchLoop() {
        if (webSocket != null) {
            logger.warn("Conductor websocket already exists");
            return;
        }

        if (isShutdown.get()) {
            logger.debug("Not starting dispatch loop as conductor is shutting down");
            return;
        }

        try {
            logger.debug("Connecting to conductor at {}", url);

            HttpClient client = HttpClient.newHttpClient();
            webSocket = client.newWebSocketBuilder()
                    .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                    .buildAsync(URI.create(url), new WebSocket.Listener() {
                        @Override
                        public void onOpen(WebSocket webSocket) {
                            logger.debug("Opened connection to DBOS conductor");
                            webSocket.request(1);
                            setPingInterval();
                        }

                        @Override
                        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
                            logger.debug("Received pong from conductor");
                            webSocket.request(1);
                            if (pingTimeout != null) {
                                pingTimeout.cancel(false);
                                pingTimeout = null;
                            }
                            return null;
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            if (isShutdown.get()) {
                                logger.info("Shutdown Conductor connection");
                            } else if (reconnectTimeout == null) {
                                logger.warn("onClose: Connection to conductor lost. Reconnecting");
                                resetWebSocket();
                            }
                            return Listener.super.onClose(webSocket, statusCode, reason);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            logger.warn("Unexpected exception in connection to conductor. Reconnecting", error);
                            resetWebSocket();
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            BaseMessage request;
                            webSocket.request(1);
                            try {
                                request = JSONUtil.fromJson(data.toString(), BaseMessage.class);
                            } catch (Exception e) {
                                logger.error("Conductor JSON Parsing error", e);
                                return CompletableFuture.completedStage(null);
                            }

                            String responseText;
                            try {
                                BaseResponse response = getResponse(request);
                                responseText = JSONUtil.toJson(response);
                            } catch (Exception e) {
                                logger.error("Conductor Response error", e);
                                return CompletableFuture.completedStage(null);
                            }

                            return webSocket.sendText(responseText, true)
                                    .exceptionally(ex -> {
                                        logger.error("Conductor sendText error", ex);
                                        return null;
                                    });
                        }
                    }).join();
        } catch (Exception e) {
            logger.warn("Error in conductor loop. Reconnecting", e);
            resetWebSocket();
        }
    }

    BaseResponse getResponse(BaseMessage message) {
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
        // TODO: real implementation
        ExecutorInfoRequest request = (ExecutorInfoRequest) message;
        return new ExecutorInfoResponse(request, new RuntimeException("not yet implemented"));
    }

    static BaseResponse handleRecovery(Conductor conductor, BaseMessage message) {
        // TODO: recoverPendingWorkflows
        RecoveryRequest request = (RecoveryRequest) message;
        return new SuccessResponse(request, new RuntimeException("not yet implemented"));
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
            ForkOptions options = ForkOptions.builder().build();
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
            ForkOptions.Builder builder = ForkOptions.builder();
            if (request.body.new_workflow_id != null) {
                builder.forkedWorkflowId(request.body.new_workflow_id);
            }
            if (request.body.application_version != null) {
                builder.applicationVersion(request.body.application_version);
            }
            WorkflowHandle<?> handle = conductor.dbosExecutor
                    .forkWorkflow(request.body.workflow_id, request.body.start_step, builder.build());
            return new ForkWorkflowResponse(request, handle.getWorkflowId());
        } catch (Exception e) {
            logger.error("Exception encountered when forking workflow {}", request, e);
            return new ForkWorkflowResponse(request, e);
        }
    }

    static BaseResponse handleListWorkflows(Conductor conductor, BaseMessage message) {
        ListWorkflowsRequest request = (ListWorkflowsRequest) message;
        try {
            ListWorkflowsInput input = request.asInput();
            List<WorkflowStatus> statuses = conductor.systemDatabase.listWorkflows(input);
            List<WorkflowsOutput> output = statuses.stream().map(s -> new WorkflowsOutput(s))
                    .collect(Collectors.toList());
            return new WorkflowOutputsResponse(request, output);
        } catch (Exception e) {
            logger.error("Exception encountered when listing workflows", e);
            return new WorkflowOutputsResponse(request, e);
        }
    }

    static BaseResponse handleListQueuedWorkflows(Conductor conductor, BaseMessage message) {
        ListQueuedWorkflowsRequest request = (ListQueuedWorkflowsRequest) message;
        return new SuccessResponse(request, new RuntimeException("not yet implemented"));
    }

    static BaseResponse handleListSteps(Conductor conductor, BaseMessage message) {
        ListStepsRequest request = (ListStepsRequest) message;
        try {
            List<StepInfo> stepInfoList = conductor.systemDatabase.listWorkflowSteps(request.workflow_id);
            List<ListStepsResponse.Step> steps = stepInfoList.stream().map(i -> new ListStepsResponse.Step(i))
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
            List<GetPendingWorkflowsOutput> pending = conductor.systemDatabase.getPendingWorkflows(request.executor_id,
                    request.application_version);
            return new ExistPendingWorkflowsResponse(request, pending.size() > 0);
        } catch (Exception e) {
            logger.error("Exception encountered when checking for pending workflows", e);
            return new ExistPendingWorkflowsResponse(request, e);
        }
    }

    static BaseResponse handleGetWorkflow(Conductor conductor, BaseMessage message) {
        GetWorkflowRequest request = (GetWorkflowRequest) message;
        try {
            WorkflowStatus status = conductor.systemDatabase.getWorkflowStatus(request.workflow_id);
            WorkflowsOutput output = status != null ? new WorkflowsOutput(status) : null;
            return new GetWorkflowResponse(request, output);
        } catch (Exception e) {
            logger.error("Exception encountered when getting workflow {}", request.workflow_id, e);
            return new GetWorkflowResponse(request, e);
        }
    }

    static BaseResponse handleRetention(Conductor conductor, BaseMessage message) {
        RetentionRequest request = (RetentionRequest) message;
        return new SuccessResponse(request, new RuntimeException("not yet implemented"));
    }
}
