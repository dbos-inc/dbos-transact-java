package dev.dbos.transact.conductor;

import dev.dbos.transact.conductor.protocol.BaseMessage;
import dev.dbos.transact.conductor.protocol.BaseResponse;
import dev.dbos.transact.conductor.protocol.CancelRequest;
import dev.dbos.transact.conductor.protocol.ExecutorInfoResponse;
import dev.dbos.transact.conductor.protocol.ExistPendingWorkflowsRequest;
import dev.dbos.transact.conductor.protocol.ExistPendingWorkflowsResponse;
import dev.dbos.transact.conductor.protocol.ForkWorkflowRequest;
import dev.dbos.transact.conductor.protocol.ForkWorkflowResponse;
import dev.dbos.transact.conductor.protocol.GetWorkflowRequest;
import dev.dbos.transact.conductor.protocol.GetWorkflowResponse;
import dev.dbos.transact.conductor.protocol.ListStepsRequest;
import dev.dbos.transact.conductor.protocol.ListStepsResponse;
import dev.dbos.transact.conductor.protocol.ListWorkflowsRequest;
import dev.dbos.transact.conductor.protocol.MessageType;
import dev.dbos.transact.conductor.protocol.RestartRequest;
import dev.dbos.transact.conductor.protocol.ResumeRequest;
import dev.dbos.transact.conductor.protocol.SuccessResponse;
import dev.dbos.transact.conductor.protocol.WorkflowOutputsResponse;
import dev.dbos.transact.conductor.protocol.WorkflowsOutput;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.http.controllers.AdminController;
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
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Conductor {

    private static Logger logger = LoggerFactory.getLogger(AdminController.class);

    private String url;
    private SystemDatabase systemDatabase;
    private DBOSExecutor dbosExecutor;
    private WebSocket webSocket;
    private final ObjectMapper mapper = new ObjectMapper();

    public Conductor(SystemDatabase s, DBOSExecutor e, String key) {
        Objects.requireNonNull(s);
        Objects.requireNonNull(e);
        Objects.requireNonNull(key);

        String appName = e.getAppName();
        Objects.requireNonNull(appName, "App Name must not be null to use Conductor");

        this.systemDatabase = s;
        this.dbosExecutor = e;

        String dbosDomain = System.getenv("DBOS_DOMAIN");
        if (dbosDomain == null || dbosDomain.trim().isEmpty()) {
            dbosDomain = "cloud.dbos.dev";
        }

        this.url = "wss://" + dbosDomain + "/conductor/v1alpha1/websocket/" + appName + "/" + key;
    }

    public CompletableFuture<Void> createWebSocket() {
        HttpClient client = HttpClient.newHttpClient();
        return client.newWebSocketBuilder().buildAsync(URI.create(url), new Listener() {

        }).thenAccept(socket -> {
            this.webSocket = socket;
        });
    }

    public void dispatchLoop() {
        if (webSocket != null) {
            logger.warn("Conductor websocket already exists");
            return;
        }

        // TODO: shutting down check

        try {
            logger.debug("Connecting to conductor at {}", this.url);

            HttpClient client = HttpClient.newHttpClient();

            webSocket = client.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .buildAsync(URI.create(url), new WebSocket.Listener() {
                        @Override
                        public void onOpen(WebSocket webSocket) {
                            logger.debug("Opened connection to DBOS conductor");
                            Listener.super.onOpen(webSocket);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            // TODO Auto-generated method stub
                            return Listener.super.onClose(webSocket, statusCode, reason);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            // TODO Auto-generated method stub
                            Listener.super.onError(webSocket, error);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            BaseMessage request;
                            try {
                                request = mapper.readValue(data.toString(), BaseMessage.class);
                            } catch (Exception e) {
                                logger.error("Conductor JSON Parsing error", e);
                                webSocket.request(1);
                                return CompletableFuture.completedStage(null);
                            }

                            String responseText;
                            try {
                                BaseResponse response = getResponse(request);
                                responseText = mapper.writeValueAsString(response);
                            } catch (Exception e) {
                                logger.error("Conductor JSON Serialization error", e);
                                webSocket.request(1);
                                return CompletableFuture.completedStage(null);
                            }

                            return webSocket.sendText(responseText, true)
                                    .exceptionally(ex -> {
                                        logger.error("Conductor sendText error", ex);
                                        return null;
                                    })
                                    .thenRun(() -> webSocket.request(1));
                        }
                    }).join();
        } catch (Exception e) {
            logger.warn("Error in conductor loop. Reconnecting", e);

        }

    }

    BaseResponse getResponse(BaseMessage message) {
        MessageType messageType = MessageType.fromValue(message.type);
        switch (messageType) {
            case EXECUTOR_INFO : {
                // TODO: real implementation
                return new ExecutorInfoResponse(message, new RuntimeException("not yet implemented"));
            }
            case RECOVERY : {
                // TODO: recoverPendingWorkflows
                return new SuccessResponse(message, new RuntimeException("not yet implemented"));
            }
            case CANCEL : {
                CancelRequest req = (CancelRequest) message;
                try {
                    dbosExecutor.cancelWorkflow(req.workflow_id);
                    return new SuccessResponse(message, true);
                } catch (Exception e) {
                    logger.error("Exception encountered when cancelling workflow {}", req.workflow_id, e);
                    return new SuccessResponse(message, e);
                }
            }
            case RESUME : {
                ResumeRequest req = (ResumeRequest) message;
                try {
                    dbosExecutor.resumeWorkflow(req.workflow_id);
                    return new SuccessResponse(message, true);
                } catch (Exception e) {
                    logger.error("Exception encountered when resuming workflow {}", req.workflow_id, e);
                    return new SuccessResponse(message, e);
                }
            }
            case RESTART : {
                RestartRequest req = (RestartRequest) message;
                try {
                    ForkOptions options = ForkOptions.builder().build();
                    dbosExecutor.forkWorkflow(req.request_id, 0, options);
                    return new SuccessResponse(message, true);
                } catch (Exception e) {
                    logger.error("Exception encountered when restarting workflow {}", req.workflow_id, e);
                    return new SuccessResponse(message, e);

                }
            }
            case FORK_WORKFLOW : {
                ForkWorkflowRequest req = (ForkWorkflowRequest) message;
                if (req.body.workflow_id == null || req.body.start_step == null) {
                    return new ForkWorkflowResponse(message, null, "Invalid Fork Workflow Request");
                }
                try {
                    ForkOptions.Builder builder = ForkOptions.builder();
                    if (req.body.new_workflow_id != null) {
                        builder.forkedWorkflowId(req.body.new_workflow_id);
                    }
                    if (req.body.application_version != null) {
                        builder.applicationVersion(req.body.application_version);
                    }
                    WorkflowHandle<?> handle = dbosExecutor
                            .forkWorkflow(req.body.workflow_id, req.body.start_step, builder.build());
                    return new ForkWorkflowResponse(message, handle.getWorkflowId());
                } catch (Exception e) {
                    logger.error("Exception encountered when forking workflow {}", req, e);
                    return new ForkWorkflowResponse(message, e);
                }
            }
            case LIST_WORKFLOWS : {
                ListWorkflowsRequest req = (ListWorkflowsRequest) message;
                try {
                    ListWorkflowsInput input = req.getInput();
                    List<WorkflowStatus> statuses = systemDatabase.listWorkflows(input);
                    List<WorkflowsOutput> output = statuses.stream().map(s -> new WorkflowsOutput(s))
                            .collect(Collectors.toList());
                    return new WorkflowOutputsResponse(message, output);
                } catch (Exception e) {
                    logger.error("Exception encountered when listing workflows", e);
                    return new WorkflowOutputsResponse(message, e);
                }
            }
            case LIST_QUEUED_WORKFLOWS : {
                // TODO: implement dbosExec.listQueuedWorkflows
                return new WorkflowOutputsResponse(message, Collections.emptyList());
            }
            case GET_WORKFLOW : {
                GetWorkflowRequest req = (GetWorkflowRequest) message;
                try {
                    WorkflowStatus status = systemDatabase.getWorkflowStatus(req.workflow_id);
                    WorkflowsOutput output = status != null ? new WorkflowsOutput(status) : null;
                    return new GetWorkflowResponse(message, output);
                } catch (Exception e) {
                    logger.error("Exception encountered when getting workflow {}", req.workflow_id, e);
                    return new GetWorkflowResponse(message, e);
                }
            }
            case EXIST_PENDING_WORKFLOWS : {
                ExistPendingWorkflowsRequest req = (ExistPendingWorkflowsRequest) message;
                try {
                    List<GetPendingWorkflowsOutput> pending = systemDatabase.getPendingWorkflows(req.executor_id,
                            req.application_version);
                    return new ExistPendingWorkflowsResponse(message, pending.size() > 0);
                } catch (Exception e) {
                    logger.error("Exception encountered when checking for pending workflows", e);
                    return new ExistPendingWorkflowsResponse(message, e);
                }
            }
            case LIST_STEPS : {
                ListStepsRequest req = (ListStepsRequest) message;
                try {
                    List<StepInfo> stepInfoList = systemDatabase.listWorkflowSteps(req.workflow_id);
                    List<ListStepsResponse.Step> steps = stepInfoList.stream().map(i -> new ListStepsResponse.Step(i))
                            .collect(Collectors.toList());
                    return new ListStepsResponse(message, steps);
                } catch (Exception e) {
                    logger.error("Exception encountered when listing steps {}", req.workflow_id, e);
                    return new ListStepsResponse(message, e);
                }
            }
            case RETENTION : {
                // TODO: implement garbage collect and global timeout
                return new SuccessResponse(message, new RuntimeException("not yet implemented"));

            }

            default :
                logger.warn("Conductor unknown message type {}", message.type);
                return new BaseResponse(message.type, message.request_id, "Unknown message type");
        }

    }

}
