package ru.phyllosedis.weatherpredict.orchestrator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@ApplicationScoped
@Path("/test")
@Produces(MediaType.APPLICATION_JSON)
public class TestResource {
    @GET
    public String get(){
        return """
                {
                    "working": "yes"
                }
                """;
    }
}
