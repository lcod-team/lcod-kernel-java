package work.lcod.kernel.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import work.lcod.kernel.runtime.KernelRegistry;
import work.lcod.kernel.runtime.ComposeLoader;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;

/**
 * Public entry point for embedding the Java kernel.
 */
public final class LcodRunner {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_REF = new TypeReference<>() {};

    public RunResult run(LcodRunConfiguration configuration) {
        var started = Instant.now();
        try {
            prepareFilesystem(configuration);
            var steps = loadCompose(configuration);
            var initialState = parseInitialState(configuration.inputPayload());
            var registry = KernelRegistry.create();
            var ctx = new ExecutionContext(registry, configuration.workingDirectory());
            var finalState = ComposeRunner.runSteps(ctx, steps, initialState, Map.of());

            var metadata = new LinkedHashMap<String, Object>();
            metadata.put("compose", configuration.composeTarget().display());
            metadata.put("result", finalState);
            metadata.put("cacheDirectory", configuration.cacheDirectory().toString());
            metadata.put("lockFile", configuration.lockFile().toString());
            metadata.put("logLevel", configuration.logLevel().name());
            metadata.put("status", "ok");
            return RunResult.success(metadata, started);
        } catch (Exception ex) {
            var errorMeta = new LinkedHashMap<String, Object>();
            errorMeta.put("compose", configuration.composeTarget().display());
            if (ex.getMessage() != null && !ex.getMessage().isBlank()) {
                errorMeta.put("error", ex.getMessage());
            }
            if (Boolean.getBoolean("lcod.debug")) {
                ex.printStackTrace();
            }
            return RunResult.failure(ex.getMessage(), errorMeta, started);
        }
    }

    public RunResult runToJson(LcodRunConfiguration configuration) {
        var result = run(configuration);
        try {
            var json = JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result.toSerializableMap());
            return result.withSerializedPayload(json);
        } catch (JsonProcessingException ex) {
            return RunResult.failure("Unable to serialize result payload: " + ex.getMessage(), Map.of(), result.startedAt());
        }
    }

    private void prepareFilesystem(LcodRunConfiguration configuration) throws IOException {
        Files.createDirectories(configuration.cacheDirectory());
        var lockParent = configuration.lockFile().getParent();
        if (lockParent != null) {
            Files.createDirectories(lockParent);
        }
    }

    private List<Map<String, Object>> loadCompose(LcodRunConfiguration configuration) {
        return configuration.composeTarget().remoteUri()
            .map(ComposeLoader::loadFromHttp)
            .orElseGet(() -> ComposeLoader.loadFromLocalFile(configuration.composeTarget().localPath().orElseThrow()));
    }

    private Map<String, Object> parseInitialState(String payload) {
        if (payload == null || payload.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            var parsed = JSON.readValue(payload, MAP_REF);
            return new LinkedHashMap<>(parsed);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Invalid JSON input payload", ex);
        }
    }

    private Registry bootstrapRegistry() {
        return KernelRegistry.create();
    }
}
