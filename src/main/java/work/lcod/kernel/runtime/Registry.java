package work.lcod.kernel.runtime;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores kernel functions and contract bindings.
 */
public final class Registry {
    static final String RAW_INPUT_KEY = "__lcod_input__";

    private final Map<String, Entry> functions = new ConcurrentHashMap<>();
    private volatile Map<String, String> bindings = Map.of();

    public Registry register(String id, KernelFunction fn) {
        return register(id, fn, null, null);
    }

    public Registry register(String id, KernelFunction fn, List<String> outputs) {
        return register(id, fn, outputs, null);
    }

    public Registry register(String id, KernelFunction fn, List<String> outputs, ComponentMetadata metadata) {
        List<String> normalized = (outputs == null || outputs.isEmpty())
            ? List.of()
            : List.copyOf(outputs);
        functions.put(id, new Entry(id, fn, normalized, metadata));
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

    public Map<String, String> bindings() {
        return bindings;
    }

    public Entry get(String id) {
        return functions.get(id);
    }

    public void unregister(String id) {
        if (id != null) {
            functions.remove(id);
        }
    }

    public Map<String, Entry> entries() {
        return Collections.unmodifiableMap(functions);
    }

    public record Entry(String id, KernelFunction function, List<String> outputs, ComponentMetadata metadata) {}
}
