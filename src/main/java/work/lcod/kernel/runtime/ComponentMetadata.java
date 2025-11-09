package work.lcod.kernel.runtime;

import java.util.Collections;
import java.util.List;

/**
 * Describes declared inputs/outputs/slots for a component (derived from lcp.toml).
 */
public final class ComponentMetadata {
    private final List<String> inputs;
    private final List<String> outputs;
    private final List<String> slots;

    public ComponentMetadata(List<String> inputs, List<String> outputs, List<String> slots) {
        this.inputs = inputs == null ? List.of() : List.copyOf(inputs);
        this.outputs = outputs == null ? List.of() : List.copyOf(outputs);
        this.slots = slots == null ? List.of() : List.copyOf(slots);
    }

    public List<String> inputs() {
        return inputs;
    }

    public List<String> outputs() {
        return outputs;
    }

    public List<String> slots() {
        return slots;
    }

    public boolean isEmpty() {
        return inputs.isEmpty() && outputs.isEmpty() && slots.isEmpty();
    }

    public static ComponentMetadata empty() {
        return new ComponentMetadata(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
}
