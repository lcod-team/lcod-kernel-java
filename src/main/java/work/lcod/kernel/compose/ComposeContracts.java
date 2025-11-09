package work.lcod.kernel.compose;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

public final class ComposeContracts {
    private ComposeContracts() {}

    public static void register(Registry registry) {
        registry.register("lcod://contract/compose/run_slot@1", ComposeContracts::runSlot);
    }

    private static Object runSlot(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        String slotName = asString(input.get("slot"));
        if (slotName == null || slotName.isBlank()) {
            throw new IllegalArgumentException("slot must be provided");
        }
        boolean optional = Boolean.TRUE.equals(input.get("optional"));
        if (optional && !slotDefined(meta, slotName)) {
            Map<String, Object> skipped = new LinkedHashMap<>();
            skipped.put("ran", Boolean.FALSE);
            skipped.put("result", null);
            return skipped;
        }
        Map<String, Object> state = asObject(input.get("state"));
        Map<String, Object> slotVars = asObject(input.get("slotVars"));
        try {
            Map<String, Object> result = ctx.runSlot(slotName, state, slotVars);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("ran", Boolean.TRUE);
            response.put("result", result);
            return response;
        } catch (Exception ex) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("ran", Boolean.TRUE);
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("message", ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
            error.put("code", "slot_execution_failed");
            response.put("error", error);
            return response;
        }
    }

    private static boolean slotDefined(StepMeta meta, String slotName) {
        if (meta == null) {
            return false;
        }
        Map<String, List<Map<String, Object>>> slots = meta.slots();
        if (slots == null) {
            return false;
        }
        List<Map<String, Object>> entries = slots.get(slotName);
        return entries != null && !entries.isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asObject(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            var copy = new LinkedHashMap<String, Object>();
            for (var entry : map.entrySet()) {
                copy.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            return copy;
        }
        return null;
    }

    private static String asString(Object value) {
        if (value instanceof String str) {
            return str;
        }
        return null;
    }
}
