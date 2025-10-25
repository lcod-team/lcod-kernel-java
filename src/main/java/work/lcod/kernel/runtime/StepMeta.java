package work.lcod.kernel.runtime;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Metadata describing a compose step invocation (children slots, collect path, etc.).
 */
public final class StepMeta {
    private final Map<String, List<Map<String, Object>>> slots;
    private final Map<String, Object> slotVars;
    private final String collectPath;

    public StepMeta(Map<String, List<Map<String, Object>>> slots, Map<String, Object> slotVars, String collectPath) {
        this.slots = slots == null ? Map.of() : Collections.unmodifiableMap(slots);
        this.slotVars = slotVars == null ? Map.of() : Collections.unmodifiableMap(slotVars);
        this.collectPath = collectPath;
    }

    public Map<String, List<Map<String, Object>>> slots() {
        return slots;
    }

    public Map<String, Object> slotVars() {
        return slotVars;
    }

    public String collectPath() {
        return collectPath;
    }
}
