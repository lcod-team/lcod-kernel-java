package work.lcod.kernel.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Outcome of an {@link LcodRunner} execution (usable by the CLI and embedding apps).
 */
public record RunResult(Status status, Map<String, Object> metadata, Instant startedAt, Instant finishedAt) {
    private static final ObjectWriter WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    public RunResult {
        metadata = Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    public static RunResult success(Map<String, Object> metadata, Instant startedAt) {
        return new RunResult(Status.SUCCESS, metadata, startedAt, Instant.now());
    }

    public static RunResult failure(String message, Map<String, Object> metadata, Instant startedAt) {
        Map<String, Object> meta = new LinkedHashMap<>(metadata);
        meta.putIfAbsent("error", message);
        return new RunResult(Status.FAILURE, meta, startedAt, Instant.now());
    }

    public static RunResult planned(Map<String, Object> metadata, Instant startedAt) {
        return new RunResult(Status.PLANNED, metadata, startedAt, Instant.now());
    }

    public RunResult withSerializedPayload(String payload) {
        Map<String, Object> meta = new LinkedHashMap<>(metadata);
        meta.put("payload", payload);
        return new RunResult(status, meta, startedAt, finishedAt);
    }

    public Map<String, Object> toSerializableMap() {
        Map<String, Object> serializable = new LinkedHashMap<>();
        serializable.put("status", status.name().toLowerCase());
        serializable.put("metadata", metadata);
        serializable.put("startedAt", startedAt.toString());
        serializable.put("finishedAt", finishedAt.toString());
        return serializable;
    }

    public String toPrettyJson() {
        try {
            return WRITER.writeValueAsString(toSerializableMap());
        } catch (Exception ex) {
            return "{\"status\":\"error\",\"message\":\"" + ex.getMessage() + "\"}";
        }
    }

    public enum Status {
        SUCCESS(0),
        FAILURE(1),
        PLANNED(0);

        private final int exitCode;

        Status(int exitCode) {
            this.exitCode = exitCode;
        }

        public int exitCode() {
            return exitCode;
        }
    }
}
