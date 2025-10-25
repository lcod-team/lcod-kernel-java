package work.lcod.kernel.runtime;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores kernel functions and contract bindings.
 */
public final class Registry {
    private final Map<String, Entry> functions = new ConcurrentHashMap<>();
    private volatile Map<String, String> bindings = Map.of();

    public Registry register(String id, KernelFunction fn) {
        functions.put(id, new Entry(id, fn));
        return this;
    }

    public Registry setBindings(Map<String, String> newBindings) {
        if (newBindings == null || newBindings.isEmpty()) {
            this.bindings = Map.of();
        } else {
            this.bindings = Collections.unmodifiableMap(new ConcurrentHashMap<>(newBindings));
        }
        return this;
    }

    public String resolveBinding(String contractId) {
        return bindings.get(contractId);
    }

    public Entry get(String id) {
        return functions.get(id);
    }

    public Map<String, Entry> entries() {
        return Collections.unmodifiableMap(functions);
    }

    public record Entry(String id, KernelFunction function) {}
}
