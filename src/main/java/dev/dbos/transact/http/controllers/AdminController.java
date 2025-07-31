package dev.dbos.transact.http.controllers;


import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowStatus;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

@Path("/")
public class AdminController {

    private SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor ;

    public AdminController(SystemDatabase s, DBOSExecutor e) {
        this.systemDatabase = s;
        this.dbosExecutor = e;
    }

    @GET
    @Path("/healthz")
    @Produces(MediaType.TEXT_PLAIN)
    public String health() {
        return "Healthy";
    }

    @GET
    @Path("/deactivate")
    @Produces(MediaType.TEXT_PLAIN)
    public String deactivate() {
        return "deactivated";
    }

    @GET
    @Path("/workflow-queues-metadata")
    @Produces(MediaType.APPLICATION_JSON)
    public Object workflowQueuesMetadata() {
        return "queuesMetadata";
    }

    @GET
    @Path("/workflows/{workflowId}/steps")
    @Produces(MediaType.APPLICATION_JSON)
    public List<StepInfo> ListSteps(@PathParam("workflowId") String workflowId) {
        return new ArrayList<>() ;
    }

    @GET
    @Path("/workflows/{workflowId}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkflowStatus> ListWorkflows(@PathParam("workflowId") String workflowId) {
        return new ArrayList<>() ;
    }

    @POST
    @Path("/recovery")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> recovery() {

        return new ArrayList<>();
    }

    @POST
    @Path("/workflows")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkflowStatus> workflows( ListWorkflowsInput input) {

        return new ArrayList<>();
    }

    @POST
    @Path("/workflows/{workflowId}/restart")
    @Produces(MediaType.APPLICATION_JSON)
    public void restart(@PathParam("workflowId") String workflowId) {


    }

    @POST
    @Path("/workflows/{workflowId}/resume")
    @Produces(MediaType.APPLICATION_JSON)
    public void resume(@PathParam("workflowId") String workflowId) {


    }

    @POST
    @Path("/workflows/{workflowId}/fork")
    @Produces(MediaType.APPLICATION_JSON)
    public void fork(@PathParam("workflowId") String workflowId) {


    }

    @POST
    @Path("/workflows/{workflowId}/cancel")
    @Produces(MediaType.APPLICATION_JSON)
    public void cancel(@PathParam("workflowId") String workflowId) {


    }


}
