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

import java.util.ArrayList;
import java.util.List;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class AdminController {

    private SystemDatabase systemDatabase;
    private DBOSExecutor dbosExecutor;
    Logger logger = LoggerFactory.getLogger(AdminController.class);

    public AdminController(SystemDatabase s, DBOSExecutor e) {
        this.systemDatabase = s;
        this.dbosExecutor = e;
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
        List<WorkflowHandle<?>> handles = dbosExecutor.recoverPendingWorkflows(executorIds);
        List<String> workflowIds = new ArrayList<>();
        for (WorkflowHandle<?> handle : handles) {
            workflowIds.add(handle.getWorkflowId());
        }
        return workflowIds;
    }

    @POST
    @Path("/dbos-garbage-collect")
    public Response garbageCollect() {
        // TODO: implement systemDatabase.garbageCollect
        return Response.serverError().build();
    }

    @POST
    @Path("/dbos-global-timeout")
    public Response globalTimeout() {
        // TODO: implement globalTimeout
        return Response.serverError().build();
    }

    @GET
    @Path("/dbos-workflow-queues-metadata")
    @Produces(MediaType.APPLICATION_JSON)
    public List<QueueMetadata> workflowQueuesMetadata() {
        List<Queue> queues = dbosExecutor.getAllQueuesSnapshot();
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
        return systemDatabase.listWorkflows(input);
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
        return systemDatabase.listWorkflowSteps(workflowId);
    }

    @POST
    @Path("/workflows/{workflowId}/restart")
    @Produces(MediaType.APPLICATION_JSON)
    public ForkWorkflowResponse restart(@PathParam("workflowId") String workflowId) {
        logger.info("Restarting workflow {} with a new ID", workflowId);
        WorkflowHandle<?> handle = dbosExecutor.forkWorkflow(workflowId, 0, null);
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
    public ForkWorkflowResponse fork(@PathParam("workflowId") String workflowId, ForkWorkflowRequest request) {
        if (request == null) {
            request = new ForkWorkflowRequest();
        }
        int startStep = (request.startStep != null) ? request.startStep : 0;
        logger.info("Forking workflow {} from step {} with a new ID", workflowId, startStep);

        ForkOptions.Builder builder = ForkOptions.builder();
        if (request.newWorkflowId != null) {
            builder.forkedWorkflowId(request.newWorkflowId);
        }
        if (request.applicationVersion != null) {
            builder.applicationVersion(request.applicationVersion);
        }
        if (request.timeoutMs != null) {
            builder.timeoutMS(request.timeoutMs);
        }

        WorkflowHandle<?> handle = dbosExecutor.forkWorkflow(workflowId, startStep, builder.build());
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

        public ForkWorkflowRequest() {
        }
    }

    public static class ForkWorkflowResponse {
        public String workflowId;

        public ForkWorkflowResponse(String workflowId) {
            this.workflowId = workflowId;
        }
    }
}
