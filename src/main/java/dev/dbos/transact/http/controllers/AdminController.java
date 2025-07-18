package dev.dbos.transact.http.controllers;


import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/")
public class AdminController {
    @GET
    @Path("/healthz")
    @Produces(MediaType.TEXT_PLAIN)
    public String health() {
        return "Healthy";
    }

}
