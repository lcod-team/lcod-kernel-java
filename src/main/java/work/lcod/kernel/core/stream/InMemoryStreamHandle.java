package work.lcod.kernel.core.stream;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Simple in-memory stream handle used by the tooling/test_checker fixture.
 */
public final class InMemoryStreamHandle {
    public static final String HANDLE_KEY = "__lcod_stream_handle__";

    private final List<byte[]> chunks;
    private final String encoding;
    private int chunkIndex = 0;
    private int offset = 0;
    private long sequence = 0;
    private boolean closed = false;

    private InMemoryStreamHandle(List<byte[]> chunks, String encoding) {
        this.chunks = chunks;
        this.encoding = encoding;
    }

    public static Map<String, Object> create(List<byte[]> chunks, String encoding) {
        var handle = new InMemoryStreamHandle(chunks, encoding);
        var wrapper = new LinkedHashMap<String, Object>();
        wrapper.put("id", "stream-" + UUID.randomUUID());
        wrapper.put("encoding", encoding);
        wrapper.put("storage", "memory");
        wrapper.put(HANDLE_KEY, handle);
        return wrapper;
    }

    public static InMemoryStreamHandle from(Object stream) {
        if (stream instanceof Map<?, ?> map && map.containsKey(HANDLE_KEY)) {
            Object handle = map.get(HANDLE_KEY);
            if (handle instanceof InMemoryStreamHandle h) {
                return h;
            }
        }
        return null;
    }

    public synchronized ReadChunk read(int maxBytes) {
        if (closed) {
            return ReadChunk.done(sequence);
        }
        while (chunkIndex < chunks.size()) {
            byte[] chunk = chunks.get(chunkIndex);
            int remaining = chunk.length - offset;
            if (remaining <= 0) {
                chunkIndex++;
                offset = 0;
                continue;
            }
            int limit = maxBytes > 0 ? Math.min(maxBytes, remaining) : remaining;
            byte[] slice = new byte[limit];
            System.arraycopy(chunk, offset, slice, 0, limit);
            offset += limit;
            long currentSeq = sequence++;
            if (offset >= chunk.length) {
                chunkIndex++;
                offset = 0;
            }
            return new ReadChunk(false, slice, currentSeq);
        }
        closed = true;
        return ReadChunk.done(sequence);
    }

    public synchronized void close() {
        closed = true;
    }

    public String encoding() {
        return encoding;
    }

    public record ReadChunk(boolean done, byte[] bytes, long sequence) {
        public static ReadChunk done(long seq) {
            return new ReadChunk(true, new byte[0], seq);
        }
    }
}
