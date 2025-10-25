package work.lcod.kernel.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

/**
 * Core value helpers used across spec fixtures (object merge, string format, JSON encode/decode, array utilities).
 */
public final class CorePrimitives {
    private static final ObjectMapper JSON = new ObjectMapper();

    private CorePrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://core/object/merge@1", CorePrimitives::objectMerge);
        registry.register("lcod://contract/core/object/merge@1", CorePrimitives::objectMerge);

        registry.register("lcod://core/string/format@1", CorePrimitives::stringFormat);
        registry.register("lcod://contract/core/string/format@1", CorePrimitives::stringFormat);

        registry.register("lcod://core/json/decode@1", CorePrimitives::jsonDecode);
        registry.register("lcod://contract/core/json/decode@1", CorePrimitives::jsonDecode);

        registry.register("lcod://core/json/encode@1", CorePrimitives::jsonEncode);
        registry.register("lcod://contract/core/json/encode@1", CorePrimitives::jsonEncode);

        registry.register("lcod://core/array/append@1", CorePrimitives::arrayAppend);
        registry.register("lcod://contract/core/array/append@1", CorePrimitives::arrayAppend);

        registry.register("lcod://core/array/length@1", CorePrimitives::arrayLength);
        registry.register("lcod://contract/core/array/length@1", CorePrimitives::arrayLength);

        registry.register("lcod://contract/core/hash/sha256@1", CorePrimitives::hashSha256);
        return registry;
    }

    private static Object objectMerge(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Map<String, Object> left = asObject(input, "left");
        Map<String, Object> right = asObject(input, "right");
        boolean deep = Boolean.TRUE.equals(input.get("deep"));
        String arrayStrategy = String.valueOf(input.getOrDefault("arrayStrategy", "replace"));
        if (!arrayStrategy.equals("replace") && !arrayStrategy.equals("concat")) {
            arrayStrategy = "replace";
        }

        var result = new LinkedHashMap<String, Object>(left);
        var conflicts = new ArrayList<String>();
        for (var entry : right.entrySet()) {
            conflicts.add(entry.getKey());
            Object leftValue = result.get(entry.getKey());
            Object rightValue = entry.getValue();
            if (deep && leftValue instanceof Map && rightValue instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> merged = (Map<String, Object>) objectMerge(
                    ctx,
                    Map.of(
                        "left", leftValue,
                        "right", rightValue,
                        "deep", true,
                        "arrayStrategy", arrayStrategy
                    ),
                    null
                );
                result.put(entry.getKey(), merged.get("value"));
                continue;
            }
            if (deep && leftValue instanceof List && rightValue instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> leftList = new ArrayList<>((List<Object>) leftValue);
                if ("concat".equals(arrayStrategy)) {
                    leftList.addAll((List<?>) rightValue);
                    result.put(entry.getKey(), leftList);
                } else {
                    result.put(entry.getKey(), new ArrayList<>((List<?>) rightValue));
                }
                continue;
            }
            result.put(entry.getKey(), cloneValue(rightValue));
        }

        return Map.of(
            "value", result,
            "conflicts", conflicts
        );
    }

    private static Object stringFormat(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Object templateValue = input.get("template");
        String template = templateValue == null ? "" : String.valueOf(templateValue);
        Map<String, Object> values = asObject(input, "values");
        String fallback = input.containsKey("fallback") ? String.valueOf(input.get("fallback")) : "";

        StringBuilder builder = new StringBuilder();
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < template.length(); i++) {
            char ch = template.charAt(i);
            if (ch == '{') {
                if (i + 1 < template.length() && template.charAt(i + 1) == '{') {
                    builder.append('{');
                    i += 1;
                    continue;
                }
                int close = template.indexOf('}', i + 1);
                if (close == -1) {
                    builder.append(template.substring(i));
                    break;
                }
                String token = template.substring(i + 1, close).trim();
                if (token.isEmpty()) {
                    missing.add(token);
                    builder.append(fallback);
                } else {
                    Object resolved = resolveToken(values, token);
                    if (resolved == null) {
                        missing.add(token);
                        builder.append(fallback);
                    } else {
                        builder.append(resolved);
                    }
                }
                i = close;
                continue;
            }
            if (ch == '}' && i + 1 < template.length() && template.charAt(i + 1) == '}') {
                builder.append('}');
                i += 1;
                continue;
            }
            builder.append(ch);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("value", builder.toString());
        if (!missing.isEmpty()) {
            result.put("missing", missing);
        }
        return result;
    }

    private static Object jsonDecode(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String text = String.valueOf(input.getOrDefault("text", ""));
        try {
            Object value = JSON.readValue(text, Object.class);
            return Map.of("value", value);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("json decode failed: " + ex.getMessage(), ex);
        }
    }

    private static Object jsonEncode(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Object value = input.get("value");
        boolean pretty = Boolean.TRUE.equals(input.get("pretty"));
        boolean sortKeys = Boolean.TRUE.equals(input.get("sortKeys"));
        try {
            var writer = JSON.writer();
            if (sortKeys) {
                writer = writer.with(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
            }
            if (pretty) {
                writer = writer.withDefaultPrettyPrinter();
            }
            String encoded = writer.writeValueAsString(value);
            return Map.of("text", encoded);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("json encode failed: " + ex.getMessage(), ex);
        }
    }

    private static Object arrayAppend(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        List<Object> target = asList(input.get("array"));
        Object value = input.containsKey("item") ? input.get("item") : input.get("value");
        if (value instanceof List<?> list) {
            target.addAll(list);
        } else {
            target.add(value);
        }
        return Map.of("value", target);
    }

    private static Object arrayLength(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        List<Object> items = asList(input.get("items"));
        return Map.of("length", items.size());
    }

    private static Object hashSha256(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        byte[] source = readInputBytes(ctx, input);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(source);
            String hex = toHex(hashed);
            String base64 = Base64.getEncoder().encodeToString(hashed);
            return Map.of(
                "hex", hex,
                "base64", base64,
                "bytes", source.length
            );
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", ex);
        }
    }

    private static byte[] readInputBytes(ExecutionContext ctx, Map<String, Object> input) {
        if (input == null || input.isEmpty()) {
            return new byte[0];
        }
        Object data = input.get("data");
        if (data != null) {
            String encoding = input.getOrDefault("encoding", "utf-8").toString();
            return decodeData(String.valueOf(data), encoding);
        }
        Object pathValue = input.get("path");
        if (pathValue != null) {
            Path resolved = ctx.workingDirectory().resolve(String.valueOf(pathValue)).normalize();
            try {
                return Files.readAllBytes(resolved);
            } catch (IOException ex) {
                throw new RuntimeException("Unable to read hash input path: " + resolved, ex);
            }
        }
        Object bytes = input.get("bytes");
        if (bytes instanceof List<?> list) {
            byte[] buffer = new byte[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object entry = list.get(i);
                int value = (entry instanceof Number number) ? number.intValue() : 0;
                buffer[i] = (byte) value;
            }
            return buffer;
        }
        return new byte[0];
    }

    private static byte[] decodeData(String text, String encoding) {
        if (encoding == null || encoding.isBlank() || encoding.equalsIgnoreCase("utf-8") || encoding.equalsIgnoreCase("utf8")) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        if (encoding.equalsIgnoreCase("base64")) {
            return Base64.getDecoder().decode(text);
        }
        if (encoding.equalsIgnoreCase("hex")) {
            int len = text.length();
            byte[] data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) ((Character.digit(text.charAt(i), 16) << 4) + Character.digit(text.charAt(i + 1), 16));
            }
            return data;
        }
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    @SuppressWarnings("unchecked")
    private static Object resolveToken(Map<String, Object> values, String token) {
        if (values == null) {
            return null;
        }
        if (!token.contains(".")) {
            return values.get(token);
        }
        Object current = values;
        for (String part : token.split("\\.")) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = map.get(part);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asObject(Map<String, Object> input, String key) {
        Object raw = input == null ? null : input.get(key);
        if (raw == null) {
            return new LinkedHashMap<>();
        }
        if (raw instanceof Map<?, ?> map) {
            var copy = new LinkedHashMap<String, Object>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), v));
            return copy;
        }
        throw new IllegalArgumentException("Expected object for " + key);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object raw) {
        if (raw == null) {
            return new ArrayList<>();
        }
        if (raw instanceof List<?> list) {
            return new ArrayList<>(list);
        }
        throw new IllegalArgumentException("Expected array input");
    }

    private static Object cloneValue(Object value) {
        if (value == null) return null;
        if (value instanceof Map<?, ?> map) {
            return asObject(Map.of("tmp", map), "tmp");
        }
        if (value instanceof List<?> list) {
            return new ArrayList<>(list);
        }
        return value;
    }
}
