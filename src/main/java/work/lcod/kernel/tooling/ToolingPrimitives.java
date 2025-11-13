package work.lcod.kernel.tooling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlArray;
import org.tomlj.TomlTable;
import work.lcod.kernel.core.stream.InMemoryStreamHandle;
import work.lcod.kernel.runtime.ComposeLoader;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.ComponentMetadata;
import work.lcod.kernel.runtime.StepMeta;

/**
 * Implements tooling contracts required by the spec fixtures (initial subset).
 */
public final class ToolingPrimitives {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final String LOG_CONTRACT_ID = "lcod://contract/tooling/log@1";
    private static final String LOG_CONTEXT_ID = "lcod://tooling/log.context@1";
    private static final String LOG_CAPTURE_ATTR = "__lcod_tooling_log_capture__";
    private static final String LOG_CAPTURE_OVERFLOW_ATTR = "__lcod_tooling_log_capture_overflow__";
    private static final int LOG_CAPTURE_LIMIT = 1024;
    private static final String LOG_SCOPE_ATTR = "__lcod_tooling_log_scope__";

    private ToolingPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://tooling/test_checker@1", ToolingPrimitives::testChecker);
        registry.register("lcod://tooling/script@1", ToolingPrimitives::scriptRunner);
        registry.register("lcod://contract/tooling/queue/bfs@1", ToolingPrimitives::queueBfs);
        registry.register("lcod://tooling/queue/bfs@0.1.0", ToolingPrimitives::queueBfs);
        registry.register("lcod://contract/tooling/array/append@1", ToolingPrimitives::arrayAppend);
        registry.register("lcod://tooling/array/append@0.1.0", ToolingPrimitives::arrayAppend);
        registry.register("lcod://contract/tooling/array/compact@1", ToolingPrimitives::arrayCompact);
        registry.register("lcod://tooling/array/compact@0.1.0", ToolingPrimitives::arrayCompact);
        registry.register("lcod://contract/tooling/array/flatten@1", ToolingPrimitives::arrayFlatten);
        registry.register("lcod://tooling/array/flatten@0.1.0", ToolingPrimitives::arrayFlatten);
        registry.register("lcod://contract/tooling/array/find_duplicates@1", ToolingPrimitives::arrayFindDuplicates);
        registry.register("lcod://tooling/array/find_duplicates@0.1.0", ToolingPrimitives::arrayFindDuplicates);
        registry.register("lcod://contract/tooling/path/join_chain@1", ToolingPrimitives::pathJoinChain);
        registry.register("lcod://tooling/path/join_chain@0.1.0", ToolingPrimitives::pathJoinChain);
        registry.register("lcod://contract/tooling/jsonl/read@1", ToolingPrimitives::jsonlRead);
        registry.register("lcod://contract/tooling/jsonl/read@1.0.0", ToolingPrimitives::jsonlRead);
        registry.register("lcod://tooling/jsonl/read@0.1.0", ToolingPrimitives::jsonlRead);
        registry.register("lcod://contract/tooling/value/is_defined@1", ToolingPrimitives::valueIsDefined);
        registry.register("lcod://tooling/value/is_defined@0.1.0", ToolingPrimitives::valueIsDefined);
        registry.register("lcod://contract/tooling/fs/read_optional@1", ToolingPrimitives::fsReadOptional);
        registry.register("lcod://contract/tooling/fs/write_if_changed@1", ToolingPrimitives::fsWriteIfChanged);
        registry.register("lcod://contract/tooling/string/ensure_trailing_newline@1", ToolingPrimitives::stringEnsureTrailingNewline);
        registry.register("lcod://tooling/string/ensure_trailing_newline@0.1.0", ToolingPrimitives::stringEnsureTrailingNewline);
        registry.register("lcod://tooling/value/is_string_nonempty@0.1.0", ToolingPrimitives::isStringNonEmpty);
        registry.register("lcod://tooling/json/stable_stringify@0.1.0", ToolingPrimitives::jsonStableStringify);
        registry.register("lcod://tooling/object/entries@0.1.0", ToolingPrimitives::objectEntries);
        registry.register("lcod://contract/tooling/resolver/resolve_dependencies@1", ToolingPrimitives::resolverResolveDependencies);
        registry.register("lcod://tooling/registry/scope@1", ToolingPrimitives::registryScope);
        registry.register("lcod://tooling/resolver/register@1", ToolingPrimitives::resolverRegister);
        registry.register("lcod://tooling/resolver/cache-dir@1", ToolingPrimitives::resolverCacheDir);
        registry.register("lcod://contract/tooling/registry/normalize_source@1", ToolingPrimitives::registryNormalizeSource);
        registry.register("lcod://contract/tooling/registry/normalize_sources@1", ToolingPrimitives::registryNormalizeSources);
        registry.register("lcod://axiom/toml/stringify@1", ToolingPrimitives::tomlStringify);
        registry.register(LOG_CONTRACT_ID, ToolingPrimitives::toolingLog);
        registry.register(LOG_CONTEXT_ID, ToolingPrimitives::logContext);
        ResolverHelperLoader.registerWorkspaceHelpers(registry);
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
            actual = sanitizeForReport(ComposeRunner.runSteps(ctx, compose, initialState, Map.of()));
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

    private static String asString(Object raw) {
        if (raw instanceof String str && !str.isBlank()) {
            return str;
        }
        return null;
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

    @SuppressWarnings("unchecked")
    private static List<Object> asListOrNull(Object raw) {
        if (raw instanceof List<?> list) {
            return (List<Object>) list;
        }
        return null;
    }

    private static boolean isCloneEnabled(Map<String, Object> input) {
        if (input == null) {
            return true;
        }
        Object clone = input.get("clone");
        if (clone instanceof Boolean bool) {
            return bool;
        }
        return true;
    }

    private static void applyArrayAppend(List<Object> target, Map<String, Object> input) {
        if (input == null) {
            return;
        }
        Object valuesRaw = input.get("values");
        if (valuesRaw instanceof List<?> values && !values.isEmpty()) {
            target.addAll(values);
        }
        if (input.containsKey("value")) {
            target.add(input.get("value"));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Object> appendInPlaceOrClone(List<?> items, Map<String, Object> input) {
        try {
            List<Object> target = (List<Object>) items;
            applyArrayAppend(target, input);
            return target;
        } catch (UnsupportedOperationException ignored) {
            List<Object> copy = new ArrayList<>(items);
            applyArrayAppend(copy, input);
            return copy;
        }
    }

    private static Object arrayAppend(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        boolean clone = isCloneEnabled(input);
        List<Object> sanitizedItems = asListOrNull(input != null ? input.get("items") : null);
        List<Object> rawItems = asListOrNull(ctx.currentRawInput().get("items"));
        List<Object> items = null;

        if (!clone) {
            List<Object> target = sanitizedItems != null ? sanitizedItems : rawItems;
            if (target != null) {
                items = appendInPlaceOrClone(target, input);
            }
        }

        System.err.printf(
            "arrayAppend debug: clone=%s sanitizedClass=%s rawClass=%s sanitizedIsNull=%s rawIsNull=%s%n",
            clone,
            sanitizedItems == null ? "null" : sanitizedItems.getClass().getName(),
            rawItems == null ? "null" : rawItems.getClass().getName(),
            sanitizedItems == null,
            rawItems == null
        );

        if (items == null) {
            List<Object> source = sanitizedItems != null ? sanitizedItems : rawItems;
            if (source != null) {
                items = new ArrayList<>(source);
                applyArrayAppend(items, input);
            } else {
                items = new ArrayList<>();
                applyArrayAppend(items, input);
            }
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

    private static Object resolverResolveDependencies(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        Path projectRoot = optionalString(input != null ? input.get("projectPath") : null) != null
            ? Paths.get(optionalString(input.get("projectPath"))).toAbsolutePath().normalize()
            : Paths.get(".").toAbsolutePath().normalize();

        Map<String, Object> normalizedConfig = asObject(input != null ? input.get("normalizedConfig") : null);
        Map<String, Object> rawConfig = asObject(input != null ? input.get("config") : null);

        Map<String, Object> sources = collectSources(normalizedConfig.get("sources"));
        if (sources.isEmpty()) {
            sources = collectSources(rawConfig.get("sources"));
        }

        Map<String, Object> rootDescriptor = asObject(input != null ? input.get("rootDescriptor") : null);
        if (rootDescriptor == null) {
            throw new IllegalArgumentException("resolve_dependencies contract requires rootDescriptor");
        }
        String rootDescriptorText = optionalString(input != null ? input.get("rootDescriptorText") : null);
        String rootId = optionalString(rootDescriptor.get("id"));
        if (rootId == null) {
            rootId = optionalString(input != null ? input.get("rootId") : null);
        }
        if (rootId == null) {
            rootId = "lcod://root/unknown@0.0.0";
        }

        List<String> rootRequires = parseRequiresFromDescriptor(rootDescriptor);
        Map<String, DescriptorEntry> descriptorCache = new LinkedHashMap<>();
        LinkedHashSet<String> visiting = new LinkedHashSet<>();
        List<Object> dependencyNodes = new ArrayList<>();
        for (String depId : rootRequires) {
            dependencyNodes.add(resolveDependencyNode(depId, projectRoot, sources, visiting, descriptorCache));
        }

        List<String> warnings = collectWarningBuckets(input);
        Map<String, Object> rootNode = new LinkedHashMap<>();
        rootNode.put("id", rootId);
        rootNode.put("requested", rootId);
        rootNode.put("resolved", rootId);
        rootNode.put("source", Map.of("type", "path", "path", projectRoot.toString()));
        rootNode.put("dependencies", dependencyNodes);
        rootNode.put("integrity", rootDescriptorText == null ? null : computeIntegrity(rootDescriptorText));

        Map<String, Object> resolverResult = new LinkedHashMap<>();
        resolverResult.put("root", rootNode);
        resolverResult.put("warnings", new ArrayList<>(warnings));

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("resolverResult", resolverResult);
        output.put("warnings", new ArrayList<>(warnings));
        return output;
    }

    private static Map<String, Object> collectSources(Object raw) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (!(raw instanceof Map<?, ?> map)) {
            return result;
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> spec) {
                result.put(String.valueOf(entry.getKey()), cloneObject(spec));
            }
        }
        return result;
    }

    private static List<String> collectWarningBuckets(Map<String, Object> input) {
        String[] keys = {
            "warnings",
            "loadWarnings",
            "indexWarnings",
            "registrationWarnings",
            "pointerWarnings"
        };
        List<String> collected = new ArrayList<>();
        for (String key : keys) {
            Object raw = input != null ? input.get(key) : null;
            if (raw instanceof List<?> list) {
                for (Object entry : list) {
                    if (entry instanceof String str && !str.isBlank()) {
                        collected.add(str);
                    }
                }
            }
        }
        return collected;
    }

    private static Map<String, Object> resolveDependencyNode(
        String depId,
        Path projectRoot,
        Map<String, Object> sources,
        LinkedHashSet<String> visiting,
        Map<String, DescriptorEntry> cache
    ) throws Exception {
        if (!visiting.add(depId)) {
            throw new IllegalArgumentException("dependency cycle detected for " + depId);
        }
        Object specRaw = sources.get(depId);
        if (!(specRaw instanceof Map<?, ?> spec)) {
            throw new IllegalArgumentException("no source specified for dependency " + depId);
        }
        Path descriptorDir = resolveSpecPath(depId, spec, projectRoot);
        DescriptorEntry descriptor = readDescriptorEntry(descriptorDir, cache);
        List<Object> childNodes = new ArrayList<>();
        for (String child : descriptor.requires) {
            childNodes.add(resolveDependencyNode(child, projectRoot, sources, visiting, cache));
        }
        visiting.remove(depId);

        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", depId);
        node.put("requested", depId);
        node.put("resolved", depId);
        node.put("source", Map.of("type", "registry", "reference", depId));
        node.put("dependencies", childNodes);
        node.put("integrity", computeIntegrity(descriptor.text));
        return node;
    }

    private static Path resolveSpecPath(String depId, Map<?, ?> spec, Path projectRoot) {
        String type = optionalString(spec.get("type"));
        if ("git".equalsIgnoreCase(type)) {
            String url = optionalString(spec.get("url"));
            if (url == null) {
                throw new IllegalArgumentException("git source for " + depId + " requires `url`");
            }
            return Paths.get(url).toAbsolutePath().normalize();
        }
        String rel = optionalString(spec.get("path"));
        if (rel == null) {
            throw new IllegalArgumentException("path source for " + depId + " requires `path`");
        }
        return projectRoot.resolve(rel).normalize();
    }

    private static DescriptorEntry readDescriptorEntry(Path componentDir, Map<String, DescriptorEntry> cache) throws IOException {
        Path manifest = componentDir.resolve("lcp.toml");
        Path canonical = manifest.toAbsolutePath().normalize();
        String cacheKey = canonical.toString();
        if (cache.containsKey(cacheKey)) {
            return cache.get(cacheKey);
        }
        String text = Files.readString(canonical, StandardCharsets.UTF_8);
        TomlParseResult result = Toml.parse(text);
        if (result.hasErrors()) {
            throw new IllegalArgumentException("unable to parse descriptor at " + canonical);
        }
        List<String> requires = new ArrayList<>();
        TomlArray depsArray = result.getArray("deps.requires");
        if (depsArray != null) {
            for (int i = 0; i < depsArray.size(); i++) {
                String value = depsArray.getString(i);
                if (value != null && !value.isBlank()) {
                    requires.add(value);
                }
            }
        }
        DescriptorEntry entry = new DescriptorEntry(requires, text);
        cache.put(cacheKey, entry);
        return entry;
    }

    private static List<String> parseRequiresFromDescriptor(Map<String, Object> descriptor) {
        Object depsRaw = descriptor.get("deps");
        if (!(depsRaw instanceof Map<?, ?> depsMap)) {
            return Collections.emptyList();
        }
        Object requiresRaw = ((Map<?, ?>) depsMap).get("requires");
        if (!(requiresRaw instanceof List<?> list)) {
            return Collections.emptyList();
        }
        List<String> requires = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof String str && !str.isBlank()) {
                requires.add(str);
            }
        }
        return requires;
    }

    private static String computeIntegrity(String text) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            builder.append(String.format("%02x", b));
        }
        return "sha256-" + builder;
    }

    private static Object objectEntries(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Map<String, Object> source = asObject(input != null ? input.get("value") : null);
        List<List<Object>> entries = new ArrayList<>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            List<Object> pair = new ArrayList<>(2);
            pair.add(entry.getKey());
            pair.add(entry.getValue());
            entries.add(pair);
        }
        return Map.of("entries", entries);
    }

    private static Object jsonlRead(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        String pathValue = optionalString(input != null ? input.get("path") : null);
        String urlValue = optionalString(input != null ? input.get("url") : null);
        if (pathValue == null) {
            if (urlValue != null) {
                throw new IllegalArgumentException("jsonl/read does not support url inputs yet");
            }
            throw new IllegalArgumentException("jsonl/read requires `path`");
        }
        if (urlValue != null) {
            throw new IllegalArgumentException("jsonl/read does not support url inputs yet");
        }

        String encodingValue = Optional.ofNullable(optionalString(input != null ? input.get("encoding") : null))
            .map(value -> value.toLowerCase(Locale.ROOT))
            .orElse("utf-8");
        if (!encodingValue.equals("utf-8") && !encodingValue.equals("utf8")) {
            throw new IllegalArgumentException("jsonl/read only supports utf-8 encoding (got " + encodingValue + ")");
        }

        Path path = Paths.get(pathValue);
        List<Object> entries = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo += 1;
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                try {
                    entries.add(JSON.readValue(trimmed, Object.class));
                } catch (IOException ex) {
                    warnings.add("invalid JSONL entry at " + path + ":" + lineNo + ": " + ex.getMessage());
                }
            }
        } catch (IOException ex) {
            throw new IOException("jsonl/read failed to read " + path + ": " + ex.getMessage(), ex);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("entries", entries);
        result.put("warnings", warnings);
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

    private static Map<String, Object> sanitizeForReport(Map<String, Object> input) {
        if (input == null) {
            return Map.of();
        }
        Map<String, Object> sanitized = new LinkedHashMap<>();
        for (var entry : input.entrySet()) {
            sanitized.put(entry.getKey(), sanitizeValue(entry.getValue()));
        }
        return sanitized;
    }

    private static Object sanitizeValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            if (map.containsKey(InMemoryStreamHandle.HANDLE_KEY)) {
                Map<String, Object> copy = new LinkedHashMap<>();
                map.forEach((k, v) -> {
                    if (!InMemoryStreamHandle.HANDLE_KEY.equals(k)) {
                        copy.put(String.valueOf(k), sanitizeValue(v));
                    }
                });
                return copy;
            }
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), sanitizeValue(v)));
            return copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(sanitizeValue(item));
            }
            return copy;
        }
        if (value instanceof InMemoryStreamHandle) {
            return Map.of("storage", "memory");
        }
        return value;
    }

    private static Object registryNormalizeSource(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Object rawEntry = input.get("entry");
        if (!(rawEntry instanceof Map<?, ?> map)) {
            return Map.of("entry", null, "warnings", List.of());
        }

        Map<String, Object> entry = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            if (e.getKey() instanceof String key) {
                entry.put(key, e.getValue());
            }
        }

        List<String> warnings = new ArrayList<>();
        String registryId = trimToString(entry.get("id"));
        if (registryId == null) {
            warnings.add("registry source entry is missing an id");
            return Map.of("entry", null, "warnings", warnings);
        }

        String registryType = trimToString(entry.get("type"));
        if (registryType == null) {
            registryType = "path";
        }

        Map<String, Object> normalized = new LinkedHashMap<>();
        normalized.put("id", registryId);
        normalized.put("type", registryType);

        Object priority = entry.get("priority");
        if (priority instanceof Number number && Double.isFinite(number.doubleValue())) {
            normalized.put("priority", number.intValue());
        }
        Object defaults = entry.get("defaults");
        if (defaults instanceof Map<?, ?> defaultsMap) {
            normalized.put("defaults", defaultsMap);
        }
        String registryPath = trimToString(entry.get("registryPath"));
        if (registryPath != null) {
            normalized.put("registryPath", registryPath);
        }
        String packagesPath = trimToString(entry.get("packagesPath"));
        if (packagesPath != null) {
            normalized.put("packagesPath", packagesPath);
        }

        Map<String, Object> normalizedEntry = switch (registryType) {
            case "path" -> {
                String pathValue = trimToString(entry.get("path"));
                if (pathValue == null) {
                    warnings.add(String.format("registry source \"%s\" (type=path) is missing \"path\"", registryId));
                    yield null;
                }
                normalized.put("path", pathValue);
                yield normalized;
            }
            case "jsonl" -> {
                String pathValue = trimToString(entry.get("path"));
                String inlineJsonl = trimToString(entry.get("jsonl"));
                if (pathValue != null) {
                    normalized.put("path", pathValue);
                    yield normalized;
                }
                if (inlineJsonl != null) {
                    normalized.put("jsonl", inlineJsonl);
                    yield normalized;
                }
                warnings.add(
                    String.format(
                        "registry source \"%s\" (type=jsonl) is missing \"path\" or inline \"jsonl\" content",
                        registryId
                    )
                );
                yield null;
            }
            case "inline" -> {
                List<?> rawLines = entry.get("lines") instanceof List<?> lines ? lines : List.of();
                List<Map<String, Object>> filtered = new ArrayList<>();
                for (Object candidate : rawLines) {
                    if (candidate instanceof Map<?, ?> candidateMap) {
                        Map<String, Object> line = new LinkedHashMap<>();
                        for (Map.Entry<?, ?> lineEntry : candidateMap.entrySet()) {
                            if (lineEntry.getKey() instanceof String key) {
                                line.put(key, lineEntry.getValue());
                            }
                        }
                        filtered.add(line);
                    }
                }
                if (filtered.isEmpty()) {
                    warnings.add(
                        String.format(
                            "registry source \"%s\" (type=inline) is missing \"lines\" entries",
                            registryId
                        )
                    );
                    yield null;
                }
                normalized.put("lines", filtered);
                String inlineJsonl = trimToString(entry.get("jsonl"));
                if (inlineJsonl != null) {
                    normalized.put("jsonl", inlineJsonl);
                }
                yield normalized;
            }
            default -> {
                warnings.add(
                    String.format("registry source \"%s\" has unsupported type \"%s\"", registryId, registryType)
                );
                yield null;
            }
        };

        return Map.of("entry", normalizedEntry, "warnings", warnings);
    }

    private static String trimToString(Object value) {
        if (!(value instanceof String str)) {
            return null;
        }
        String trimmed = str.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Object registryNormalizeSources(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Object rawEntries = input.get("entries");
        List<?> entries = rawEntries instanceof List<?> list ? list : List.of();
        List<Object> normalizedEntries = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        for (Object candidate : entries) {
            Map<String, Object> wrapper = Map.of("entry", candidate);
            @SuppressWarnings("unchecked")
            Map<String, Object> result =
                (Map<String, Object>) registryNormalizeSource(ctx, wrapper, meta);
            Object normalizedEntry = result.get("entry");
            if (normalizedEntry instanceof Map<?, ?>) {
                normalizedEntries.add(normalizedEntry);
            }
            Object warningList = result.get("warnings");
            if (warningList instanceof List<?>) {
                for (Object warning : (List<?>) warningList) {
                    if (warning instanceof String str && !str.isEmpty()) {
                        warnings.add(str);
                    }
                }
            }
        }
        return Map.of("entries", normalizedEntries, "warnings", warnings);
    }

    private static Object registryScope(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        Map<String, String> originalBindings = ctx.registry().bindings();
        Map<String, String> overrides = sanitizeBindings(input == null ? null : input.get("bindings"));
        boolean adjustedBindings = !overrides.isEmpty();
        if (adjustedBindings) {
            Map<String, String> merged = new LinkedHashMap<>(originalBindings);
            merged.putAll(overrides);
            ctx.registry().setBindings(merged);
        }
        List<String> registeredComponents = registerInlineComponents(ctx, input == null ? null : input.get("components"));
        try {
            List<Map<String, Object>> children = meta == null ? List.of() : meta.slots().getOrDefault("children", List.of());
            if (children.isEmpty()) {
                return Map.of();
            }
            Map<String, Object> localState = new LinkedHashMap<>();
            Map<String, Object> result = ctx.runChildren(children, localState, Map.of());
            return result == null ? Map.of() : result;
        } finally {
            if (adjustedBindings) {
                ctx.registry().setBindings(originalBindings);
            }
            for (String componentId : registeredComponents) {
                ctx.registry().unregister(componentId);
            }
        }
    }

    private static Map<String, String> sanitizeBindings(Object raw) {
        if (!(raw instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, String> sanitized = new LinkedHashMap<>();
        for (var entry : map.entrySet()) {
            String key = optionalString(entry.getKey());
            String value = optionalString(entry.getValue());
            if (key != null && value != null) {
                sanitized.put(key, value);
            }
        }
        return sanitized;
    }

    private static List<String> registerInlineComponents(ExecutionContext ctx, Object rawComponents) {
        if (!(rawComponents instanceof List<?> list) || list.isEmpty()) {
            return List.of();
        }
        List<String> registeredIds = new ArrayList<>();
        for (Object entryObj : list) {
            if (!(entryObj instanceof Map<?, ?> component)) continue;
            String id = optionalString(component.get("id"));
            if (id == null || ctx.registry().get(id) != null) {
                continue;
            }
            ComponentMetadata inlineMetadata = buildInlineMetadata(component);
            if ("lcod://impl/testing/log-capture@1".equals(id)) {
                ctx.registry().register(id, (innerCtx, payload, meta) -> {
                    Map<String, Object> entry = cloneObject(payload);
                    appendCapturedLog(innerCtx, entry);
                    return entry;
                }, null, inlineMetadata);
                registeredIds.add(id);
                continue;
            }
            if ("lcod://impl/testing/log-captured@1".equals(id)) {
                ctx.registry().register(id, (innerCtx, payload, meta) -> {
                    List<Map<String, Object>> captured = getCapturedLogs(innerCtx);
                    List<Map<String, Object>> copy = new ArrayList<>();
                    for (Map<String, Object> log : captured) {
                        copy.add(cloneObject(log));
                    }
                    return copy;
                }, null, inlineMetadata);
                registeredIds.add(id);
                continue;
            }
            Object compose = component.get("compose");
            if (!(compose instanceof List<?> stepList) || stepList.isEmpty()) {
                continue;
            }
            List<Map<String, Object>> storedSteps = new ArrayList<>();
            for (Object step : stepList) {
                if (step instanceof Map<?, ?> map) {
                    storedSteps.add(cloneObject(map));
                }
            }
            ctx.registry().register(id, (innerCtx, payload, meta) -> {
                Map<String, Object> seed = cloneObject(payload);
                return ComposeRunner.runSteps(innerCtx, storedSteps, seed, Map.of());
            }, null, inlineMetadata);
            registeredIds.add(id);
        }

        return registeredIds;
    }

    private static ComponentMetadata buildInlineMetadata(Map<?, ?> component) {
        List<String> inputs = extractMetadataKeys(component.get("inputs"));
        List<String> outputs = extractMetadataKeys(component.get("outputs"));
        List<String> slots = extractMetadataKeys(component.get("slots"));
        if (inputs.isEmpty() && outputs.isEmpty() && slots.isEmpty()) {
            return null;
        }
        return new ComponentMetadata(inputs, outputs, slots);
    }

    private static List<String> extractMetadataKeys(Object raw) {
        if (!(raw instanceof Map<?, ?> map) || map.isEmpty()) {
            return List.of();
        }
        List<String> keys = new ArrayList<>();
        for (Object key : map.keySet()) {
            String name = optionalString(key);
            if (name != null && !name.isBlank()) {
                keys.add(name);
            }
        }
        return keys;
    }

    private static Object toolingLog(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        Map<String, Object> entry = buildLogEntry(ctx, input);
        String binding = ctx.registry().resolveBinding(LOG_CONTRACT_ID);
        if (binding != null && !LOG_CONTRACT_ID.equals(binding)) {
            return ctx.call(binding, entry, meta);
        }
        appendCapturedLog(ctx, entry);
        return entry;
    }

    private static Object logContext(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        Map<String, Object> tags = sanitizeLogTags(input == null ? null : input.get("tags"));
        pushLogScope(ctx, tags);
        try {
            List<Map<String, Object>> children = meta == null ? List.of() : meta.slots().getOrDefault("children", List.of());
            if (children.isEmpty()) {
                return Map.of();
            }
            Map<String, Object> localState = new LinkedHashMap<>();
            Map<String, Object> result = ctx.runChildren(children, localState, Map.of());
            return result == null ? Map.of() : result;
        } finally {
            popLogScope(ctx);
        }
    }

    private static Map<String, Object> buildLogEntry(ExecutionContext ctx, Map<String, Object> input) {
        Map<String, Object> entry = new LinkedHashMap<>();
        String level = optionalString(input == null ? null : input.get("level"));
        entry.put("level", level == null ? "info" : level.toLowerCase(Locale.ROOT));
        String message = optionalString(input == null ? null : input.get("message"));
        if (message == null) {
            throw new IllegalArgumentException("log message is required");
        }
        entry.put("message", message);
        Object data = input == null ? null : input.get("data");
        if (data instanceof Map<?, ?> map) {
            entry.put("data", cloneObject(map));
        }
        entry.put("timestamp", Instant.now().toString());
        Map<String, Object> combinedTags = aggregateLogTags(ctx, sanitizeLogTags(input == null ? null : input.get("tags")));
        if (!combinedTags.isEmpty()) {
            entry.put("tags", combinedTags);
        }
        return entry;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> aggregateLogTags(ExecutionContext ctx, Map<String, Object> requestTags) {
        Map<String, Object> aggregated = new LinkedHashMap<>();
        Object rawStack = ctx.getAttribute(LOG_SCOPE_ATTR);
        if (rawStack instanceof Deque<?> stack) {
            for (Map<String, Object> scope : (Deque<Map<String, Object>>) stack) {
                aggregated.putAll(scope);
            }
        }
        if (requestTags != null) {
            aggregated.putAll(requestTags);
        }
        return aggregated;
    }

    private static Map<String, Object> sanitizeLogTags(Object raw) {
        if (!(raw instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> tags = new LinkedHashMap<>();
        for (var entry : map.entrySet()) {
            String key = optionalString(entry.getKey());
            Object value = entry.getValue();
            if (key == null) continue;
            if (value instanceof String || value instanceof Number || value instanceof Boolean) {
                tags.put(key, value);
            }
        }
        return tags;
    }

    @SuppressWarnings("unchecked")
    private static void appendCapturedLog(ExecutionContext ctx, Map<String, Object> entry) {
        List<Map<String, Object>> logs = (List<Map<String, Object>>) ctx.getAttribute(LOG_CAPTURE_ATTR);
        if (logs == null) {
            logs = new ArrayList<>();
            ctx.setAttribute(LOG_CAPTURE_ATTR, logs);
        }
        if (logs.size() >= LOG_CAPTURE_LIMIT) {
            Boolean overflowed = (Boolean) ctx.getAttribute(LOG_CAPTURE_OVERFLOW_ATTR);
            if (!Boolean.TRUE.equals(overflowed)) {
                Map<String, Object> notice = new LinkedHashMap<>();
                notice.put("level", "warn");
                notice.put("message", "Spec log buffer truncated");
                notice.put("tags", Map.of(
                    "component", "kernel",
                    "scope", "registry-scope",
                    "reason", "log-overflow"
                ));
                logs.add(notice);
                ctx.setAttribute(LOG_CAPTURE_OVERFLOW_ATTR, Boolean.TRUE);
            }
            return;
        }
        logs.add(cloneObject(entry));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getCapturedLogs(ExecutionContext ctx) {
        List<Map<String, Object>> logs = (List<Map<String, Object>>) ctx.getAttribute(LOG_CAPTURE_ATTR);
        return logs == null ? List.of() : logs;
    }

    @SuppressWarnings("unchecked")
    private static void pushLogScope(ExecutionContext ctx, Map<String, Object> tags) {
        Deque<Map<String, Object>> stack = (Deque<Map<String, Object>>) ctx.getAttribute(LOG_SCOPE_ATTR);
        if (stack == null) {
            stack = new ArrayDeque<>();
            ctx.setAttribute(LOG_SCOPE_ATTR, stack);
        }
        if (tags == null) {
            stack.push(Map.of());
        } else {
            stack.push(tags);
        }
    }

    @SuppressWarnings("unchecked")
    private static void popLogScope(ExecutionContext ctx) {
        Deque<Map<String, Object>> stack = (Deque<Map<String, Object>>) ctx.getAttribute(LOG_SCOPE_ATTR);
        if (stack != null && !stack.isEmpty()) {
            stack.pop();
        }
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

    private static Object resolverRegister(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        List<?> components = input != null && input.get("components") instanceof List<?> list ? list : List.of();
        List<String> warnings = new ArrayList<>();
        Registry registry = ctx.registry();
        int count = 0;
        for (Object rawComponent : components) {
            if (!(rawComponent instanceof Map<?, ?> map)) {
                continue;
            }
            Map<String, Object> component = new LinkedHashMap<>();
            map.forEach((k, v) -> component.put(String.valueOf(k), v));
            String rawId = asString(component.get("id"));
            if (rawId == null) {
                warnings.add("resolver/register: missing component id");
                continue;
            }
            String canonicalId = rawId;
            Path composePath = null;
            List<Map<String, Object>> steps;
            Object inlineCompose = component.get("compose");
            if (inlineCompose instanceof List<?> composeList) {
                try {
                    steps = castCompose(composeList);
                } catch (IllegalArgumentException ex) {
                    warnings.add("resolver/register: invalid compose definition for " + canonicalId + ": " + ex.getMessage());
                    continue;
                }
            } else {
                String composePathStr = asString(component.get("composePath"));
                if (composePathStr == null) {
                    warnings.add("resolver/register: component " + canonicalId + " missing compose data");
                    continue;
                }
                composePath = ctx.workingDirectory().resolve(composePathStr).normalize();
                try {
                    steps = ComposeLoader.loadFromLocalFile(composePath);
                } catch (Exception ex) {
                    warnings.add("resolver/register: failed to load compose for " + canonicalId + ": " + ex.getMessage());
                    continue;
                }
            }
            List<String> declaredOutputs = extractDeclaredOutputs(component, composePath);
            final List<String> allowedOutputs = declaredOutputs.isEmpty() ? null : declaredOutputs;
            final List<Map<String, Object>> componentSteps = steps;
            registry.register(canonicalId, (childCtx, childInput, childMeta) -> {
                Map<String, Object> initial = childInput == null ? new LinkedHashMap<>() : new LinkedHashMap<>(childInput);
                return ComposeRunner.runSteps(childCtx, componentSteps, initial, Map.of());
            }, allowedOutputs);
            count += 1;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("count", count);
        result.put("warnings", warnings);
        return result;
    }

    private static Object resolverCacheDir(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        String rawProject = input != null && input.get("projectPath") instanceof String str && !str.isBlank()
            ? str
            : ctx.workingDirectory().toString();
        Path projectRoot = Paths.get(rawProject);
        if (!projectRoot.isAbsolute()) {
            projectRoot = ctx.workingDirectory().resolve(projectRoot).normalize();
        }
        Path cacheDir = projectRoot.resolve(".lcod").resolve("cache");
        Files.createDirectories(cacheDir);
        return Map.of("path", cacheDir.toString());
    }

    private static Object tomlStringify(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Object rawValue = input != null ? input.get("value") : null;
        if (!(rawValue instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("value must be an object");
        }
        StringBuilder builder = new StringBuilder();
        writeTomlEntries(toObjectMap(map), "", "", builder);
        return Map.of("text", builder.toString());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> toObjectMap(Map<?, ?> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(String.valueOf(key), value));
        return copy;
    }

    private static void writeTomlEntries(Map<String, Object> map, String prefix, String indent, StringBuilder builder) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String dottedKey = prefix.isBlank() ? key : prefix + "." + key;
            if (value instanceof Map<?, ?> child) {
                writeTomlEntries(toObjectMap(child), dottedKey, indent, builder);
            } else if (value instanceof List<?> list) {
                if (isArrayOfTables(list)) {
                    String tableName = dottedKey;
                    for (Object item : list) {
                        builder.append(System.lineSeparator());
                        builder.append(indent).append("[[").append(tableName).append("]]").append(System.lineSeparator());
                        writeTomlEntries(toObjectMap((Map<?, ?>) item), "", indent + "  ", builder);
                    }
                } else {
                    builder.append(indent)
                        .append(dottedKey)
                        .append(" = ")
                        .append(formatTomlArray(list))
                        .append(System.lineSeparator());
                }
            } else {
                builder.append(indent)
                    .append(dottedKey)
                    .append(" = ")
                    .append(formatTomlLiteral(value))
                    .append(System.lineSeparator());
            }
        }
    }

    private static boolean isArrayOfTables(List<?> list) {
        if (list.isEmpty()) {
            return false;
        }
        for (Object item : list) {
            if (!(item instanceof Map<?, ?>)) {
                return false;
            }
        }
        return true;
    }

    private static String formatTomlArray(List<?> items) {
        if (items.isEmpty()) {
            return "[ ]";
        }
        List<String> formatted = new ArrayList<>();
        for (Object item : items) {
            formatted.add(formatTomlLiteral(item));
        }
        return "[ " + String.join(", ", formatted) + " ]";
    }

    private static String formatTomlLiteral(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Boolean bool) {
            return bool ? "true" : "false";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        String text = value.toString();
        String escaped = text
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r");
        return "\"" + escaped + "\"";
    }
    
    private static List<String> extractDeclaredOutputs(Map<String, Object> component, Path composePath) {
        Object outputsField = component.get("outputs");
        if (outputsField instanceof List<?> list) {
            List<String> outputs = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof String str && !str.isEmpty()) {
                    outputs.add(str);
                }
            }
            if (!outputs.isEmpty()) {
                return outputs;
            }
        }
        return loadManifestOutputs(composePath);
    }

    private static final class DescriptorEntry {
        final List<String> requires;
        final String text;

        DescriptorEntry(List<String> requires, String text) {
            this.requires = requires;
            this.text = text;
        }
    }

    private static List<String> loadManifestOutputs(Path composePath) {
        if (composePath == null) {
            return Collections.emptyList();
        }
        Path parent = composePath.getParent();
        if (parent == null) {
            return Collections.emptyList();
        }
        Path manifestPath = parent.resolve("lcp.toml");
        if (!Files.exists(manifestPath)) {
            return Collections.emptyList();
        }
        try {
            TomlParseResult result = Toml.parse(Files.readString(manifestPath));
            if (result.hasErrors()) {
                return Collections.emptyList();
            }
            TomlTable outputs = result.getTable("outputs");
            if (outputs == null) {
                return Collections.emptyList();
            }
            List<String> keys = new ArrayList<>();
            for (String key : outputs.keySet()) {
                keys.add(key);
            }
            return keys;
        } catch (Exception ex) {
            return Collections.emptyList();
        }
    }
}
