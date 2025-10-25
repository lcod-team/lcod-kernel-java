package work.lcod.kernel.tooling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
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
        registry.register("lcod://contract/tooling/array/append@1", ToolingPrimitives::arrayAppend);
        registry.register("lcod://contract/tooling/array/compact@1", ToolingPrimitives::arrayCompact);
        registry.register("lcod://contract/tooling/array/flatten@1", ToolingPrimitives::arrayFlatten);
        registry.register("lcod://contract/tooling/array/find_duplicates@1", ToolingPrimitives::arrayFindDuplicates);
        registry.register("lcod://contract/tooling/path/join_chain@1", ToolingPrimitives::pathJoinChain);
        registry.register("lcod://contract/tooling/value/is_defined@1", ToolingPrimitives::valueIsDefined);
        registry.register("lcod://contract/tooling/fs/read_optional@1", ToolingPrimitives::fsReadOptional);
        registry.register("lcod://contract/tooling/fs/write_if_changed@1", ToolingPrimitives::fsWriteIfChanged);
        registry.register("lcod://contract/tooling/string/ensure_trailing_newline@1", ToolingPrimitives::stringEnsureTrailingNewline);
        registry.register("lcod://tooling/value/is_string_nonempty@0.1.0", ToolingPrimitives::isStringNonEmpty);
        registry.register("lcod://tooling/json/stable_stringify@0.1.0", ToolingPrimitives::jsonStableStringify);
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
                warnings.add("queue/bfs key slot failed: " + describeException(ex));
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
            warnings.add("queue/bfs fallback key serialization failed: " + describeException(ex));
        }
        return "item:" + iterations;
    }

    private static String describeException(Exception ex) {
        if (ex == null) {
            return "<unknown>";
        }
        String type = ex.getClass().getSimpleName();
        String message = ex.getMessage();
        if (message == null || message.isBlank()) {
            return type;
        }
        return type + ": " + message;
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

    private static Object arrayAppend(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        List<Object> items = asList(input != null ? input.get("items") : null);
        List<Object> values = asList(input != null ? input.get("values") : null);
        items.addAll(values);
        if (input != null && input.containsKey("value")) {
            items.add(input.get("value"));
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("items", items);
        response.put("length", items.size());
        return response;
    }

    private static Object arrayCompact(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        List<Object> items = asList(input != null ? input.get("items") : null);
        List<Object> values = new ArrayList<>();
        for (Object entry : items) {
            if (entry != null) {
                values.add(entry);
            }
        }
        return Map.of("values", values);
    }

    private static Object arrayFlatten(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        List<Object> items = asList(input != null ? input.get("items") : null);
        List<Object> values = new ArrayList<>();
        for (Object entry : items) {
            if (entry instanceof List<?> list) {
                for (Object nested : list) {
                    if (nested != null) {
                        values.add(nested);
                    }
                }
            } else if (entry != null) {
                values.add(entry);
            }
        }
        return Map.of("values", values);
    }

    private static Object arrayFindDuplicates(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        List<Object> items = asList(input != null ? input.get("items") : null);
        Set<String> seen = new LinkedHashSet<>();
        Set<String> duplicates = new LinkedHashSet<>();
        for (Object entry : items) {
            if (!(entry instanceof String str)) continue;
            if (!seen.add(str)) {
                duplicates.add(str);
            }
        }
        return Map.of("duplicates", new ArrayList<>(duplicates));
    }

    private static Object pathJoinChain(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String base = optionalString(input != null ? input.get("base") : null);
        List<Object> segments = asList(input != null ? input.get("segments") : null);
        Path current = base == null || base.isBlank() ? null : Paths.get(base);
        for (Object segmentRaw : segments) {
            String segment = optionalString(segmentRaw);
            if (segment == null) continue;
            Path candidate = Paths.get(segment);
            if (candidate.isAbsolute()) {
                current = candidate;
            } else if (current == null) {
                current = candidate;
            } else {
                current = current.resolve(candidate);
            }
        }
        String normalized = current == null ? "" : normalizePath(current);
        return Map.of("path", normalized);
    }

    private static Object valueIsDefined(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        boolean ok = input != null && input.containsKey("value") && input.get("value") != null;
        return Map.of("ok", ok);
    }

    private static Object fsReadOptional(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String pathValue = optionalString(input != null ? input.get("path") : null);
        String encoding = Optional.ofNullable(optionalString(input != null ? input.get("encoding") : null)).orElse("utf-8");
        String fallback = optionalString(input != null ? input.get("fallback") : null);
        String warningMessage = optionalString(input != null ? input.get("warningMessage") : null);
        Map<String, Object> result = new LinkedHashMap<>();
        if (pathValue == null) {
            result.put("text", fallback);
            result.put("exists", false);
            result.put("warning", warningMessage);
            return result;
        }
        Path path = Paths.get(pathValue);
        try {
            String text = Files.readString(path, charsetFor(encoding));
            result.put("text", text);
            result.put("exists", true);
            result.put("warning", null);
        } catch (IOException ex) {
            result.put("text", fallback);
            result.put("exists", false);
            String warning = warningMessage != null ? warningMessage : ex.getMessage();
            result.put("warning", warning);
        }
        return result;
    }

    private static Object fsWriteIfChanged(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        String pathValue = optionalString(input != null ? input.get("path") : null);
        if (pathValue == null) {
            throw new IllegalArgumentException("write_if_changed: path is required");
        }
        String encoding = Optional.ofNullable(optionalString(input != null ? input.get("encoding") : null)).orElse("utf-8");
        Object rawContent = input != null ? input.get("content") : null;
        String content = rawContent == null ? "" : String.valueOf(rawContent);
        Path path = Paths.get(pathValue);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        String previous = null;
        if (Files.exists(path)) {
            previous = Files.readString(path, charsetFor(encoding));
        }
        if (previous != null && previous.equals(content)) {
            return Map.of("changed", false);
        }
        Files.writeString(path, content, charsetFor(encoding));
        return Map.of("changed", true);
    }

    private static Object stringEnsureTrailingNewline(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String text = Optional.ofNullable(optionalString(input != null ? input.get("text") : null)).orElse("");
        String newline = Optional.ofNullable(optionalString(input != null ? input.get("newline") : null)).orElse("\n");
        if (newline.isEmpty() || text.endsWith(newline)) {
            return Map.of("text", text);
        }
        return Map.of("text", text + newline);
    }

    private static Object isStringNonEmpty(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String value = optionalString(input != null ? input.get("value") : null);
        boolean ok = value != null && !value.isEmpty();
        return Map.of("ok", ok);
    }

    private static Object jsonStableStringify(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Object value = input != null ? input.get("value") : null;
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            Object canonical = canonicalize(value);
            String text = JSON.writer()
                .with(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .writeValueAsString(canonical);
            result.put("text", text);
            result.put("warning", null);
        } catch (Exception ex) {
            result.put("text", null);
            result.put("warning", ex.getMessage());
        }
        return result;
    }

    private static Object canonicalize(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            for (var entry : map.entrySet()) {
                String key = String.valueOf(entry.getKey());
                sorted.put(key, canonicalize(entry.getValue()));
            }
            return sorted;
        }
        if (value instanceof List<?> list) {
            List<Object> normalized = new ArrayList<>();
            for (Object item : list) {
                normalized.add(canonicalize(item));
            }
            return normalized;
        }
        return value;
    }

    private static String optionalString(Object value) {
        if (value == null) return null;
        if (value instanceof String str) {
            return str.isBlank() ? null : str;
        }
        String converted = String.valueOf(value);
        return converted.isBlank() ? null : converted;
    }

    private static Charset charsetFor(String encoding) {
        try {
            return Charset.forName(encoding);
        } catch (Exception ignored) {
            return StandardCharsets.UTF_8;
        }
    }

    private static String normalizePath(Path path) {
        String normalized = path.normalize().toString().replace('\\', '/');
        if (normalized.equals(".")) {
            return "";
        }
        return normalized;
    }

    private static Object scriptRunner(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        Map<String, Object> payload = input == null ? Map.of() : input;
        return ScriptRuntime.run(ctx, payload, meta);
    }
}
