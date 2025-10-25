package work.lcod.kernel.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Public entry point for embedding the Java kernel.
 */
public final class LcodRunner {
    private static final ObjectWriter WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    public RunResult run(LcodRunConfiguration configuration) {
        Instant started = Instant.now();
        try {
            Files.createDirectories(configuration.cacheDirectory());
            if (configuration.lockFile().getParent() != null) {
                Files.createDirectories(configuration.lockFile().getParent());
            }
        } catch (IOException ex) {
            return RunResult.failure(
                "Failed to prepare filesystem layout: " + ex.getMessage(),
                Map.of("error", ex.getClass().getSimpleName()),
                started
            );
        }

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("compose", configuration.composeTarget().display());
        metadata.put("cacheMode", configuration.cacheMode().name());
        metadata.put("cacheDirectory", configuration.cacheDirectory().toString());
        metadata.put("lockFile", configuration.lockFile().toString());
        metadata.put("forceResolve", configuration.forceResolve());
        metadata.put("timeoutMs", configuration.timeout().map(t -> t.toMillis()).orElse(null));
        metadata.put("logLevel", configuration.logLevel().name());
        metadata.put("message", "Java kernel skeleton placeholder. Execution engine not implemented yet.");

        return RunResult.planned(metadata, started);
    }

    public RunResult runToJson(LcodRunConfiguration configuration) {
        RunResult result = run(configuration);
        try {
            String json = WRITER.writeValueAsString(result.toSerializableMap());
            return result.withSerializedPayload(json);
        } catch (JsonProcessingException ex) {
            return RunResult.failure("Unable to serialize result payload: " + ex.getMessage(), Map.of(), result.startedAt());
        }
    }
}
