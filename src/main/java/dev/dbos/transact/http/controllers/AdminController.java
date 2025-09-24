package dev.dbos.transact.http.controllers;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.QueueMetadata;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class AdminController {

  private static final Logger logger = LoggerFactory.getLogger(AdminController.class);

  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;
  private final List<Queue> queues;

  public AdminController(DBOSExecutor exec, SystemDatabase sysDb, List<Queue> queues) {
    this.systemDatabase = sysDb;
    this.dbosExecutor = exec;
    this.queues = queues;
  }

  @GET
  @Path("/dbos-healthz")
  @Produces(MediaType.TEXT_PLAIN)
  public String health() {
    return "healthy";
  }

  @GET
  @Path("/dbos-perf")
  public Response perf() {
    // TODO: implement perf hooks
    return Response.serverError().build();
  }

  @GET
  @Path("/dbos-deactivate")
  public Response deactivate() {
    // TODO: implement dbosExec.deactivateEventReceivers
    return Response.serverError().build();
  }

  @POST
  @Path("/dbos-workflow-recovery")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> recovery(List<String> executorIds) {
    logger.info("Recovering workflows for executors {}", executorIds);
    List<WorkflowHandle<?, ?>> handles = dbosExecutor.recoverPendingWorkflows(executorIds);
    List<String> workflowIds = new ArrayList<>();
    for (WorkflowHandle<?, ?> handle : handles) {
      workflowIds.add(handle.getWorkflowId());
    }
    return workflowIds;
  }

  @POST
  @Path("/dbos-garbage-collect")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response garbageCollect(GarbageCollectRequest request) {
    systemDatabase.garbageCollect(request.cutoff_epoch_timestamp_ms, request.rows_threshold);
    return Response.noContent().build();
  }

  @POST
  @Path("/dbos-global-timeout")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response globalTimeout(GlobalTimeoutRequest request) {
    dbosExecutor.globalTimeout(request.cutoff_epoch_timestamp_ms);
    return Response.noContent().build();
  }

  @GET
  @Path("/dbos-workflow-queues-metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public List<QueueMetadata> workflowQueuesMetadata() {
    List<QueueMetadata> metadataList = new ArrayList<QueueMetadata>();
    for (Queue queue : queues) {
      metadataList.add(new QueueMetadata(queue));
    }
    return metadataList;
  }

  @POST
  @Path("/queues")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response listQueuedWorkflow() {
    // TODO: implement dbosExec.listQueuedWorkflows
    return Response.serverError().build();
  }

  @POST
  @Path("/workflows")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    if (input == null) {
      input = new ListWorkflowsInput();
    }
    return dbosExecutor.listWorkflows(input);
  }

  @GET
  @Path("/workflows/{workflowId}")
  @Produces(MediaType.APPLICATION_JSON)
  public WorkflowStatus GetWorkflowStatus(@PathParam("workflowId") String workflowId) {
    logger.info("Get workflow status for workflow {}", workflowId);
    return systemDatabase.getWorkflowStatus(workflowId);
  }

  @GET
  @Path("/workflows/{workflowId}/steps")
  @Produces(MediaType.APPLICATION_JSON)
  public List<StepInfo> ListSteps(@PathParam("workflowId") String workflowId) {
    logger.info("Retrieving steps for workflow {}", workflowId);
    return dbosExecutor.listWorkflowSteps(workflowId);
  }

  @POST
  @Path("/workflows/{workflowId}/restart")
  @Produces(MediaType.APPLICATION_JSON)
  public ForkWorkflowResponse restart(@PathParam("workflowId") String workflowId) {
    logger.info("Restarting workflow {} with a new ID", workflowId);
    WorkflowHandle<?, ?> handle = dbosExecutor.forkWorkflow(workflowId, 0, null);
    return new ForkWorkflowResponse(handle.getWorkflowId());
  }

  @POST
  @Path("/workflows/{workflowId}/resume")
  @Produces(MediaType.APPLICATION_JSON)
  public Response resume(@PathParam("workflowId") String workflowId) {
    logger.info("Resuming workflow {}", workflowId);
    dbosExecutor.resumeWorkflow(workflowId);
    return Response.noContent().build();
  }

  @POST
  @Path("/workflows/{workflowId}/fork")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public ForkWorkflowResponse fork(
      @PathParam("workflowId") String workflowId, ForkWorkflowRequest request) {
    if (request == null) {
      request = new ForkWorkflowRequest();
    }
    int startStep = (request.startStep != null) ? request.startStep : 0;
    logger.info("Forking workflow {} from step {} with a new ID", workflowId, startStep);

    Duration timeout = request.timeoutMs != null ? Duration.ofMillis(request.timeoutMs) : null;
    var options = new ForkOptions(request.newWorkflowId, request.applicationVersion, timeout);

    WorkflowHandle<?, ?> handle = dbosExecutor.forkWorkflow(workflowId, startStep, options);
    return new ForkWorkflowResponse(handle.getWorkflowId());
  }

  @POST
  @Path("/workflows/{workflowId}/cancel")
  public Response cancel(@PathParam("workflowId") String workflowId) {
    logger.info("Cancel workflow {}", workflowId);
    dbosExecutor.cancelWorkflow(workflowId);
    return Response.noContent().build();
  }

  public static class ForkWorkflowRequest {
    public Integer startStep;
    public String newWorkflowId;
    public String applicationVersion;
    public Long timeoutMs;

    public ForkWorkflowRequest() {}
  }

  public static class ForkWorkflowResponse {
    public String workflowId;

    public ForkWorkflowResponse(String workflowId) {
      this.workflowId = workflowId;
    }
  }

  public static class GarbageCollectRequest {
    public Long cutoff_epoch_timestamp_ms;
    public Long rows_threshold;

    public GarbageCollectRequest() {}
  }

  public static class GlobalTimeoutRequest {
    public Long cutoff_epoch_timestamp_ms;

    public GlobalTimeoutRequest() {}
  }
}
