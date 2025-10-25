package work.lcod.kernel.core;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import work.lcod.kernel.core.stream.InMemoryStreamHandle;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

/**
 * Minimal stream helpers supporting the spec foreach stream fixtures.
 */
public final class CoreStreamPrimitives {
    private CoreStreamPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://contract/core/stream/read@1", CoreStreamPrimitives::read);
        registry.register("lcod://contract/core/stream/close@1", CoreStreamPrimitives::close);
        return registry;
    }

    private static Object read(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Object streamValue = input == null ? null : input.get("stream");
        if (!(streamValue instanceof Map<?, ?> streamMap)) {
            throw new IllegalArgumentException("stream handle is required");
        }
        InMemoryStreamHandle handle = InMemoryStreamHandle.from(streamMap);
        if (handle == null) {
            throw new IllegalStateException("Unsupported stream handle");
        }
        int maxBytes = input != null && input.get("maxBytes") instanceof Number num ? Math.max(1, num.intValue()) : 0;
        var chunk = handle.read(maxBytes);
        if (chunk.done()) {
            return Map.of("done", true, "stream", streamMap);
        }
        String decode = input != null && input.get("decode") != null ? String.valueOf(input.get("decode")) : handle.encoding();
        String normalizedEncoding = decode == null ? "utf-8" : decode.toLowerCase();
        String chunkValue = encodeChunk(chunk.bytes(), normalizedEncoding);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("done", false);
        result.put("chunk", chunkValue);
        result.put("encoding", normalizedEncoding);
        result.put("bytes", chunk.bytes().length);
        result.put("seq", chunk.sequence());
        result.put("stream", streamMap);
        return result;
    }

    private static Object close(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        Object streamValue = input == null ? null : input.get("stream");
        if (streamValue instanceof Map<?, ?> map) {
            InMemoryStreamHandle handle = InMemoryStreamHandle.from(map);
            if (handle != null) {
                handle.close();
            }
        }
        return Map.of("closed", true);
    }

    private static String encodeChunk(byte[] bytes, String encoding) {
        if (encoding == null || encoding.equalsIgnoreCase("utf-8") || encoding.equalsIgnoreCase("utf8")) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (encoding.equalsIgnoreCase("base64")) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (encoding.equalsIgnoreCase("hex")) {
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
