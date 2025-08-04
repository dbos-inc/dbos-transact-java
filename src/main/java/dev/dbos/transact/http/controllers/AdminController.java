package dev.dbos.transact.http.controllers;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
    @Path("/deactivate")
    @Produces(MediaType.TEXT_PLAIN)
    public String deactivate() {
        // TODO: this endpoint deactivates the system for new workflows
        return "deactivated";
    }

    @GET
    @Path("/dbos-workflow-queues-metadata")
    @Produces(MediaType.APPLICATION_JSON)
    public Object workflowQueuesMetadata() {
        // TODO: this endpoint returns metadata about the workflow queues
        return "queuesMetadata";
    }

    @GET
    @Path("/workflows/{workflowId}/steps")
    @Produces(MediaType.APPLICATION_JSON)
    public List<StepInfo> ListSteps(@PathParam("workflowId") String workflowId) {
        logger.info("Retrieving steps for workflow {}", workflowId);
        return systemDatabase.listWorkflowSteps(workflowId);
    }

    @GET
    @Path("/workflows/{workflowId}")
    @Produces(MediaType.APPLICATION_JSON)
    public WorkflowStatus GetWorkflowStatus(@PathParam("workflowId") String workflowId) {
        logger.info("Get workflow status for workflow {}", workflowId);
        return systemDatabase.getWorkflowStatus(workflowId);
    }

    @POST
    @Path("/dbos-workflow-recovery")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> recovery(List<String> executorIds) {
        // TODO: this endpoint takes a JSON string array of executor IDs and calls dbosExec.recoverPendingWorkflows
        return new ArrayList<>();
    }

    @POST
    @Path("/workflows")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkflowStatus> workflows(ListWorkflowsInput input) {
        try {
            return systemDatabase.listWorkflows(input);
        } catch (java.sql.SQLException e) {
            logger.error("Error listing workflows", e);
            throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
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
    @Produces(MediaType.APPLICATION_JSON)
    public ForkWorkflowResponse fork(@PathParam("workflowId") String workflowId, ForkWorkflowRequest request) {
        logger.info("Forking workflow {} from step {} with a new ID", workflowId, request.startStep);
        int startStep = (request.startStep != null) ? request.startStep : 0;
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
        private Integer startStep;
        private String newWorkflowId;
        private String applicationVersion;
        private Long timeoutMs;

        public ForkWorkflowRequest() { }
    }

    public static class ForkWorkflowResponse {
        public String workflowId;

        public ForkWorkflowResponse(String workflowId) {
            this.workflowId = workflowId;
        }
    }
}
