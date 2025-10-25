package work.lcod.kernel.runtime;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Execution context passed to kernel functions. Handles registry access, slot orchestration, and scope cleanups.
 */
public final class ExecutionContext {
    private final Registry registry;
    private final CancellationToken cancellationToken;
    private final Deque<List<Runnable>> scopeStack = new ArrayDeque<>();
    private ChildRunner childRunner = (steps, localState, slotVars) -> {
        throw new IllegalStateException("runChildren is unavailable in this context");
    };
    private SlotRunner slotRunner = (name, localState, slotVars) -> {
        throw new IllegalStateException("runSlot is unavailable in this context");
    };

    public ExecutionContext(Registry registry) {
        this(registry, new CancellationToken());
    }

    public ExecutionContext(Registry registry, CancellationToken token) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.cancellationToken = token == null ? new CancellationToken() : token;
    }

    public Registry registry() {
        return registry;
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
        var safeInput = input == null ? Map.<String, Object>of() : input;
        return entry.function().invoke(this, safeInput, meta);
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
}
