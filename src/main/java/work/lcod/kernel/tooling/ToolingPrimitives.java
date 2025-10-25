package work.lcod.kernel.tooling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import work.lcod.kernel.core.stream.InMemoryStreamHandle;
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
        registry.register("lcod://tooling/queue/bfs@0.1.0", ToolingPrimitives::queueBfs);
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
        registerStreams(initialState, input.get("streams"));
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

    private static void registerStreams(Map<String, Object> state, Object specs) {
        if (!(specs instanceof List<?> list) || state == null) {
            return;
        }
        for (Object specObj : list) {
            if (!(specObj instanceof Map<?, ?> map)) continue;
            Object target = map.get("target");
            Object chunksObj = map.get("chunks");
            if (!(target instanceof String targetPath) || !(chunksObj instanceof List<?> chunkList)) {
                continue;
            }
            String encoding = map.get("encoding") instanceof String str && !str.isBlank() ? str.toLowerCase() : "utf-8";
            List<byte[]> decoded = new ArrayList<>();
            for (Object chunk : chunkList) {
                decoded.add(decodeChunk(String.valueOf(chunk), encoding));
            }
            Map<String, Object> handle = InMemoryStreamHandle.create(decoded, encoding);
            setDeepValue(state, targetPath, handle);
        }
    }

    private static byte[] decodeChunk(String value, String encoding) {
        return switch (encoding) {
            case "base64" -> java.util.Base64.getDecoder().decode(value);
            case "hex" -> decodeHex(value);
            default -> value.getBytes(StandardCharsets.UTF_8);
        };
    }

    private static byte[] decodeHex(String value) {
        int len = value.length();
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            bytes[i / 2] = (byte) ((Character.digit(value.charAt(i), 16) << 4) + Character.digit(value.charAt(i + 1), 16));
        }
        return bytes;
    }

    private static void setDeepValue(Map<String, Object> target, String path, Object value) {
        if (path == null || path.isBlank()) return;
        String[] parts = path.replace("$", "").replaceFirst("^\\.", "").split("\\.");
        Map<String, Object> cursor = target;
        for (int i = 0; i < parts.length - 1; i++) {
            String key = parts[i];
            Object next = cursor.get(key);
            if (!(next instanceof Map<?, ?> nextMap)) {
                var fresh = new LinkedHashMap<String, Object>();
                cursor.put(key, fresh);
                cursor = fresh;
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> casted = (Map<String, Object>) nextMap;
                cursor = casted;
            }
        }
        cursor.put(parts[parts.length - 1], value);
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

    private static Object queueBfs(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        Deque<Object> queue = new ArrayDeque<>(asList(input == null ? null : input.get("items")));
        Map<String, Object> visitedMap = cloneObject(input == null ? null : input.get("visited"));
        Set<String> visited = new LinkedHashSet<>(visitedMap.keySet());
        Map<String, Object> state = cloneObject(input == null ? null : input.get("state"));
        Object contextValue = input == null ? null : input.get("context");
        long maxIterations = parsePositiveLong(input == null ? null : input.get("maxIterations"), Long.MAX_VALUE);
        long iterations = 0;
        List<String> warnings = new ArrayList<>();

        while (!queue.isEmpty()) {
            ctx.ensureNotCancelled();
            if (iterations >= maxIterations) {
                throw new IllegalStateException("queue/bfs exceeded maxIterations (" + maxIterations + ")");
            }
            Object item = queue.removeFirst();

            Map<String, Object> slotVars = new LinkedHashMap<>();
            slotVars.put("index", iterations);
            slotVars.put("remaining", queue.size());
            slotVars.put("visitedCount", visited.size());
            slotVars.put("item", item);
            slotVars.put("state", state);
            if (contextValue != null) {
                slotVars.put("context", contextValue);
            }

            Map<String, Object> slotPayload = new LinkedHashMap<>();
            slotPayload.put("item", item);
            slotPayload.put("state", state);
            if (contextValue != null) {
                slotPayload.put("context", contextValue);
            }

            String key = null;
            try {
                Map<String, Object> keyResult = ctx.runSlot("key", new LinkedHashMap<>(slotPayload), slotVars);
                key = extractKeyString(keyResult);
            } catch (Exception ex) {
                warnings.add("queue/bfs key slot failed: " + ex.getMessage());
            }
            if (key == null || key.isBlank()) {
                key = fallbackKey(item, iterations, warnings);
            }
            if (visited.contains(key)) {
                iterations++;
                continue;
            }
            visited.add(key);
            visitedMap.put(key, Boolean.TRUE);

            Map<String, Object> processResult = ctx.runSlot("process", slotPayload, slotVars);
            Object newState = processResult.get("state");
            if (newState instanceof Map<?, ?> map) {
                state = cloneObject(map);
            }
            Object children = processResult.get("children");
            if (children instanceof List<?> list) {
                for (Object child : list) {
                    queue.add(child);
                }
            }
            Object warningValues = processResult.get("warnings");
            if (warningValues instanceof List<?> list) {
                for (Object warning : list) {
                    if (warning != null) {
                        warnings.add(String.valueOf(warning));
                    }
                }
            }
            iterations++;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("state", state);
        result.put("visited", visitedMap);
        result.put("warnings", warnings);
        result.put("iterations", iterations);
        return result;
    }

    private static String extractKeyString(Map<String, Object> keyResult) {
        if (keyResult == null) {
            return null;
        }
        Object keyValue = keyResult.get("key");
        if (keyValue instanceof String str && !str.isBlank()) {
            return str;
        }
        if (keyResult.size() == 1) {
            Object single = keyResult.values().iterator().next();
            if (single instanceof String str && !str.isBlank()) {
                return str;
            }
        }
        return null;
    }

    private static String fallbackKey(Object item, long iterations, List<String> warnings) {
        try {
            String serialized = JSON.writeValueAsString(item);
            if (serialized != null && !serialized.isBlank()) {
                return serialized;
            }
        } catch (Exception ex) {
            warnings.add("queue/bfs fallback key serialization failed: " + ex.getMessage());
        }
        return "item:" + iterations;
    }

    private static long parsePositiveLong(Object value, long fallback) {
        if (value instanceof Number num) {
            long candidate = num.longValue();
            return candidate > 0 ? candidate : fallback;
        }
        if (value instanceof String str) {
            try {
                long candidate = Long.parseLong(str.trim());
                return candidate > 0 ? candidate : fallback;
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    private static List<Object> asList(Object raw) {
        if (raw instanceof List<?> list) {
            return new ArrayList<>(list);
        }
        return new ArrayList<>();
    }

    private static Object scriptRunner(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        Map<String, Object> payload = input == null ? Map.of() : input;
        return ScriptRuntime.run(ctx, payload, meta);
    }
}
