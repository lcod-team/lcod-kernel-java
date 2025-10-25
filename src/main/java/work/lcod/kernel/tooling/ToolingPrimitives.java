package work.lcod.kernel.tooling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import work.lcod.kernel.runtime.ComposeLoader;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

/**
 * Implements tooling contracts required by the spec fixtures (initial subset).
 */
public final class ToolingPrimitives {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private ToolingPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://tooling/test_checker@1", ToolingPrimitives::testChecker);
        registry.register("lcod://tooling/script@1", ToolingPrimitives::scriptRunner);
        return registry;
    }

    private static Object testChecker(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        if (input == null) {
            throw new IllegalArgumentException("tooling/test_checker requires an input object");
        }
        Map<String, Object> expected = asObject(input.get("expected"));
        if (expected == null) {
            throw new IllegalArgumentException("expected result must be provided");
        }
        Map<String, Object> initialState = cloneObject(input.get("input"));
        List<Map<String, Object>> compose = resolveCompose(ctx, input);

        Instant started = Instant.now();
        Map<String, Object> actual;
        boolean success = false;
        List<String> messages = new ArrayList<>();
        try {
            actual = ComposeRunner.runSteps(ctx, compose, initialState, Map.of());
            success = matchesExpected(actual, expected);
            if (!success) {
                messages.add("Actual output differs from expected output");
            }
        } catch (Exception ex) {
            messages.add("Compose execution failed: " + ex.getMessage());
            actual = Map.of("error", Map.of("message", ex.getMessage()));
        }

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("success", success);
        report.put("actual", actual);
        report.put("expected", expected);
        report.put("durationMs", Duration.between(started, Instant.now()).toMillis());
        if (!messages.isEmpty()) {
            report.put("messages", messages);
        }
        if (!success) {
            report.put("diffs", List.of(Map.of(
                "path", "$",
                "actual", truncate(actual),
                "expected", truncate(expected)
            )));
        }
        return report;
    }

    private static List<Map<String, Object>> resolveCompose(ExecutionContext ctx, Map<String, Object> input) throws IOException {
        Object inline = input.get("compose");
        if (inline instanceof List<?> list) {
            return castCompose(list);
        }
        Object ref = input.get("composeRef");
        if (ref instanceof Map<?, ?> map) {
            Object pathValue = map.get("path");
            if (pathValue instanceof String str && !str.isBlank()) {
                Path resolved = ctx.workingDirectory().resolve(str).normalize();
                try {
                    return ComposeLoader.loadFromLocalFile(resolved);
                } catch (IllegalStateException ex) {
                    // fallback: handle YAML docs without compose root
                    var tree = YAML.readTree(Files.readString(resolved));
                    var composeNode = tree.get("compose");
                    if (composeNode == null || !composeNode.isArray()) {
                        throw ex;
                    }
                    List<Map<String, Object>> steps = new ArrayList<>();
                    composeNode.elements().forEachRemaining(node -> {
                        try {
                            steps.add(JSON.readValue(JSON.writeValueAsBytes(node), Map.class));
                        } catch (IOException ioException) {
                            throw new RuntimeException(ioException);
                        }
                    });
                    return steps;
                }
            }
        }
        throw new IllegalArgumentException("compose or composeRef.path must be provided");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> castCompose(List<?> raw) {
        List<Map<String, Object>> steps = new ArrayList<>();
        for (Object obj : raw) {
            if (obj instanceof Map<?, ?> map) {
                steps.add((Map<String, Object>) map);
            } else {
                throw new IllegalArgumentException("compose entries must be objects");
            }
        }
        return steps;
    }

    private static Map<String, Object> asObject(Object raw) {
        if (raw == null) {
            return Map.of();
        }
        if (raw instanceof Map<?, ?> map) {
            var copy = new LinkedHashMap<String, Object>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), v));
            return copy;
        }
        throw new IllegalArgumentException("expected value must be an object");
    }

    private static Map<String, Object> cloneObject(Object raw) {
        if (raw == null) {
            return new LinkedHashMap<>();
        }
        if (raw instanceof Map<?, ?> map) {
            var copy = new LinkedHashMap<String, Object>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), v));
            return copy;
        }
        throw new IllegalArgumentException("input payload must be an object");
    }

    private static boolean matchesExpected(Object actual, Object expected) {
        if (actual == expected) {
            return true;
        }
        if (actual == null || expected == null) {
            return false;
        }
        if (actual instanceof Map<?, ?> actualMap && expected instanceof Map<?, ?> expectedMap) {
            for (var entry : expectedMap.entrySet()) {
                var key = entry.getKey();
                if (!matchesExpected(actualMap.get(key), entry.getValue())) {
                    return false;
                }
            }
            return true;
        }
        if (actual instanceof List<?> actualList && expected instanceof List<?> expectedList) {
            if (actualList.size() != expectedList.size()) {
                return false;
            }
            for (int i = 0; i < actualList.size(); i += 1) {
                if (!matchesExpected(actualList.get(i), expectedList.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return actual.equals(expected);
    }

    private static String truncate(Object value) {
        try {
            String json = JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value);
            if (json.length() <= 2000) {
                return json;
            }
            return json.substring(0, 2000) + "…";
        } catch (Exception ex) {
            return String.valueOf(value);
        }
    }

    private static Object scriptRunner(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        Map<String, Object> payload = input == null ? Map.of() : input;
        return ScriptRuntime.run(ctx, payload, meta);
    }
}