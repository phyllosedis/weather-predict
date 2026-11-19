package ru.phyllosedis.weatherpredict.orchestrator.api;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import ru.phyllosedis.weatherpredict.orchestrator.service.RabbitCsvOrchestrator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

@jakarta.ws.rs.Path("/weather")
@ApplicationScoped
public class WeatherResource {
    @Inject
    private RabbitCsvOrchestrator orchestrator;

    @POST
    @jakarta.ws.rs.Path("process")
    @Produces(MediaType.APPLICATION_JSON)
    public Response processDataset(
            @QueryParam("file") @DefaultValue("data/open-meteo-55.78N37.58E151m.csv") String filePath,
            @QueryParam("chunkSize") @DefaultValue("20000") int chunkSize) {
        try {
            long start = System.currentTimeMillis();
            Path input = Path.of(filePath).toAbsolutePath().normalize();

            if (!Files.exists(input)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Файл не найден: " + input))
                        .build();
            }

            Path output = input.getParent().resolve("processed_" + input.getFileName().toString());
            orchestrator.processCsvFile(input, output, chunkSize);

            long elapsedMs = System.currentTimeMillis() - start;
            return Response.ok(Map.of(
                    "status", "SUCCESS",
                    "inputFile", input.toString(),
                    "outputFile", output.toString(),
                    "elapsedMs", elapsedMs
            )).build();

        } catch (Exception e) {
            return Response.serverError()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }
}
