package work.lcod.kernel.runtime;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Objects;
import java.util.Optional;

/**
 * Execution context passed to kernel functions. Handles registry access, slot orchestration, and scope cleanups.
 */
public final class ExecutionContext {
    private final Registry registry;
    private final Path workingDirectory;
    private final CancellationToken cancellationToken;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();
    private final Deque<List<Runnable>> scopeStack = new ArrayDeque<>();
    private ChildRunner childRunner = (steps, localState, slotVars) -> {
        throw new IllegalStateException("runChildren is unavailable in this context");
    };
    private final SlotRunner defaultSlotRunner = (name, localState, slotVars) -> {
        throw new IllegalStateException("runSlot is unavailable in this context");
    };
    private SlotRunner slotRunner = defaultSlotRunner;
    private final Deque<Map<String, Object>> rawInputStack = new ArrayDeque<>();

    public ExecutionContext(Registry registry) {
        this(registry, null, new CancellationToken());
    }

    public ExecutionContext(Registry registry, Path workingDirectory) {
        this(registry, workingDirectory, new CancellationToken());
    }

    public ExecutionContext(Registry registry, Path workingDirectory, CancellationToken token) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.workingDirectory = workingDirectory == null
            ? Paths.get("").toAbsolutePath().normalize()
            : workingDirectory.toAbsolutePath().normalize();
        this.cancellationToken = token == null ? new CancellationToken() : token;
    }

    public Registry registry() {
        return registry;
    }

    public Path workingDirectory() {
        return workingDirectory;
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void setAttribute(String key, Object value) {
        if (value == null) {
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    public void ensureNotCancelled() {
        if (cancellationToken.isCancelled()) {
            throw new KernelCancellationException("Execution cancelled");
        }
    }

    public void cancel() {
        cancellationToken.cancel();
    }

    public Object call(String id, Map<String, Object> input, StepMeta meta) throws Exception {
        ensureNotCancelled();
        var entry = registry.get(id);
        if (entry == null && id != null && id.startsWith("lcod://contract/")) {
            var impl = registry.resolveBinding(id);
            if (impl != null) {
                entry = registry.get(impl);
            }
        }
        if (entry == null) {
            throw new IllegalStateException("Function not registered: " + id);
        }
        PreparedInput prepared = prepareInput(input, entry.metadata());
        if (prepared.raw() != null) {
            rawInputStack.push(prepared.raw());
        }
        Object result;
        try {
            result = entry.function().invoke(this, prepared.sanitized(), meta);
        } finally {
            if (prepared.raw() != null) {
                rawInputStack.pop();
            }
        }
        if (entry.outputs() != null && !entry.outputs().isEmpty() && result instanceof Map<?, ?> map) {
            result = filterOutputs(map, entry.outputs());
        }
        return result;
    }

    ChildRunner childRunner() {
        return childRunner;
    }

    void setChildRunner(ChildRunner runner) {
        this.childRunner = runner;
    }

    SlotRunner slotRunner() {
        return slotRunner;
    }

    void setSlotRunner(SlotRunner runner) {
        this.slotRunner = runner;
    }

    boolean isDefaultSlotRunner(SlotRunner runner) {
        return runner == defaultSlotRunner;
    }

    public Map<String, Object> runChildren(List<Map<String, Object>> steps, Map<String, Object> localState, Map<String, Object> slotVars) throws Exception {
        ensureNotCancelled();
        return childRunner.runChildren(steps, localState, slotVars);
    }

    public Map<String, Object> runSlot(String slotName, Map<String, Object> localState, Map<String, Object> slotVars) throws Exception {
        ensureNotCancelled();
        return slotRunner.runSlot(slotName, localState, slotVars);
    }

    void pushScope() {
        scopeStack.push(new ArrayList<>());
    }

    void popScope() {
        var cleaners = scopeStack.poll();
        if (cleaners == null) {
            return;
        }
        for (int i = cleaners.size() - 1; i >= 0; i--) {
            var cleanup = cleaners.get(i);
            try {
                cleanup.run();
            } catch (Exception ex) {
                // ignore cleanup errors for now
            }
        }
    }

    public void defer(Runnable cleanup) {
        if (scopeStack.isEmpty()) {
            scopeStack.push(new ArrayList<>());
        }
        scopeStack.peek().add(cleanup);
    }

    public interface ChildRunner {
        Map<String, Object> runChildren(List<Map<String, Object>> steps, Map<String, Object> localState, Map<String, Object> slotVars) throws Exception;
    }

    public interface SlotRunner {
        Map<String, Object> runSlot(String slotName, Map<String, Object> localState, Map<String, Object> slotVars) throws Exception;
    }

    public static final class CancellationToken {
        private volatile boolean cancelled = false;

        public void cancel() {
            this.cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }

    public static final class KernelCancellationException extends RuntimeException {
        public KernelCancellationException(String message) {
            super(message);
        }
    }

    private static Map<String, Object> filterOutputs(Map<?, ?> state, List<String> allowed) {
        Map<String, Object> filtered = new LinkedHashMap<>();
        for (String key : allowed) {
            Object value = state != null && state.containsKey(key) ? state.get(key) : null;
            filtered.put(key, value);
        }
        return filtered;
    }

    public Map<String, Object> currentRawInputSnapshot() {
        Map<String, Object> snapshot = rawInputStack.peek();
        if (snapshot == null) {
            return Map.of();
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> copy = (Map<String, Object>) deepCopy(snapshot);
        return copy;
    }

    private PreparedInput prepareInput(Map<String, Object> input, ComponentMetadata metadata) {
        Map<String, Object> sanitized;
        Map<String, Object> raw = null;
        if (metadata == null || metadata.inputs().isEmpty()) {
            sanitized = input == null ? new LinkedHashMap<>() : new LinkedHashMap<>(input);
        } else {
            Map<String, Object> base = input == null ? Map.of() : input;
            @SuppressWarnings("unchecked")
            Map<String, Object> rawCopy = (Map<String, Object>) deepCopy(base);
            raw = rawCopy;
            sanitized = new LinkedHashMap<>();
            for (String key : metadata.inputs()) {
                Object value = base.containsKey(key) ? base.get(key) : null;
                sanitized.put(key, deepCopy(value));
            }
        }
        return new PreparedInput(sanitized, raw);
    }

    private static Object deepCopy(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                copy.put(String.valueOf(entry.getKey()), deepCopy(entry.getValue()));
            }
            return copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(deepCopy(item));
            }
            return copy;
        }
        return value;
    }
    private record PreparedInput(Map<String, Object> sanitized, Map<String, Object> raw) {}
}
