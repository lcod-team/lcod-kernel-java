package work.lcod.kernel.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Port of the JS compose runtime that interprets compose steps sequentially with slot support.
 */
public final class ComposeRunner {
    private static final String SPREAD_KEY = "__lcod_spreads__";
    private static final String OPTIONAL_FLAG = "__lcod_optional__";

    private ComposeRunner() {}

    public static Map<String, Object> runSteps(ExecutionContext ctx, List<Map<String, Object>> rawSteps, Map<String, Object> initialState, Map<String, Object> slotVars) throws Exception {
        Map<String, Object> state = initialState == null ? new LinkedHashMap<>() : new LinkedHashMap<>(initialState);
        List<Map<String, Object>> steps = rawSteps == null ? List.of() : rawSteps;

        for (int index = 0; index < steps.size(); index++) {
            ctx.ensureNotCancelled();
            Map<String, Object> step = steps.get(index);
            if (step == null) continue;
            Map<String, List<Map<String, Object>>> slotMap = normalizeSlotMap(step);
            Map<String, List<Map<String, Object>>> childrenMeta = slotMap == null ? Map.of() : new LinkedHashMap<>(slotMap);
            if (childrenMeta.containsKey("body") && !childrenMeta.containsKey("children")) {
                childrenMeta.put("children", childrenMeta.get("body"));
            }

            ExecutionContext.ChildRunner previousChildren = ctx.childRunner();
            ExecutionContext.SlotRunner previousSlotRunner = ctx.slotRunner();
            ctx.setChildRunner((children, localState, slotOverrides) -> {
                ctx.ensureNotCancelled();
                Map<String, Object> base = localState == null ? state : localState;
                ctx.pushScope();
                try {
                    return runSteps(ctx, safeSteps(children), base, slotOverrides == null ? slotVars : slotOverrides);
                } finally {
                    ctx.popScope();
                }
            });
            ctx.setSlotRunner((name, localState, slotOverrides) -> {
                ctx.ensureNotCancelled();
                List<Map<String, Object>> target = resolveSlotSteps(slotMap, name);
                Map<String, Object> base = localState == null ? state : localState;
                ctx.pushScope();
                try {
                    return runSteps(ctx, target, base, slotOverrides == null ? slotVars : slotOverrides);
                } finally {
                    ctx.popScope();
                }
            });

            Map<String, Object> input = buildInput(castMap(step.get("in")), state, slotVars);
            Object result;
            try {
                ctx.pushScope();
                String callId = Objects.toString(step.get("call"), null);
                result = ctx.call(callId, input, new StepMeta(childrenMeta, slotVars, Objects.toString(step.get("collectPath"), null)));
            } finally {
                ctx.popScope();
                ctx.setChildRunner(previousChildren);
                ctx.setSlotRunner(previousSlotRunner);
            }
            applyOutputs(step, state, result);
        }

        return state;
    }

    private static List<Map<String, Object>> safeSteps(List<Map<String, Object>> steps) {
        return steps == null ? List.of() : steps;
    }

    private static Map<String, List<Map<String, Object>>> normalizeSlotMap(Map<String, Object> step) {
        if (step == null) return null;
        Map<String, List<Map<String, Object>>> slots = new LinkedHashMap<>();
        mergeSlotContainer(slots, step.get("children"), "children");
        mergeSlotContainer(slots, step.get("slots"), null);
        return slots.isEmpty() ? null : slots;
    }

    @SuppressWarnings("unchecked")
    private static void mergeSlotContainer(Map<String, List<Map<String, Object>>> target, Object source, String defaultName) {
        if (source == null) return;
        if (source instanceof List<?> list) {
            target.put(defaultName == null ? "children" : defaultName, castStepList(list));
            return;
        }
        if (source instanceof Map<?, ?> map) {
            if (defaultName != null) {
                Object maybeList = map.get(defaultName);
                if (maybeList instanceof List<?> list) {
                    target.put(defaultName, castStepList(list));
                }
            }
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getValue() instanceof List<?> list) {
                    target.put(String.valueOf(entry.getKey()), castStepList(list));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> castStepList(List<?> list) {
        List<Map<String, Object>> steps = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map<?, ?> map) {
                steps.add((Map<String, Object>) map);
            }
        }
        return steps;
    }

    private static List<Map<String, Object>> resolveSlotSteps(Map<String, List<Map<String, Object>>> slotMap, String name) {
        if (slotMap == null) return List.of();
        if (slotMap.containsKey(name)) {
            return slotMap.get(name);
        }
        if ("children".equals(name) && slotMap.containsKey("body")) {
            return slotMap.get("body");
        }
        if ("body".equals(name) && slotMap.containsKey("children")) {
            return slotMap.get("children");
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object obj) {
        if (obj instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return Map.of();
    }

    private static Map<String, Object> buildInput(Map<String, Object> bindings, Map<String, Object> state, Map<String, Object> slot) {
        Map<String, Object> result = new LinkedHashMap<>();
        Object spreads = bindings.get(SPREAD_KEY);
        if (spreads instanceof List<?> descriptors) {
            for (Object descriptorObj : descriptors) {
                if (!(descriptorObj instanceof Map<?, ?> descriptor)) continue;
                Object sourceNode = resolveValue(descriptor.get("source"), state, slot);
                Map<String, Object> payload = asObject(sourceNode);
                if (payload == null) {
                    if (!Boolean.TRUE.equals(descriptor.get("optional"))) {
                        continue;
                    }
                    continue;
                }
                Object pick = descriptor.get("pick");
                if (pick instanceof List<?> pickList) {
                    for (Object keyObj : pickList) {
                        String key = String.valueOf(keyObj);
                        if (payload.containsKey(key)) {
                            result.put(key, cloneLiteral(payload.get(key)));
                        } else if (!Boolean.TRUE.equals(descriptor.get("optional"))) {
                            result.put(key, null);
                        }
                    }
                } else {
                    for (Map.Entry<String, Object> entry : payload.entrySet()) {
                        result.put(entry.getKey(), cloneLiteral(entry.getValue()));
                    }
                }
            }
        }

        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            if (SPREAD_KEY.equals(entry.getKey())) continue;
            Object value = entry.getValue();
            boolean optional = false;
            if (value instanceof Map<?, ?> map && Boolean.TRUE.equals(map.get(OPTIONAL_FLAG))) {
                optional = true;
                value = map.get("value");
            }
            Object resolved = resolveValue(value, state, slot);
            if (optional && (resolved == null)) {
                continue;
            }
            result.put(entry.getKey(), resolved);
        }
        return result;
    }

    private static Object resolveValue(Object value, Map<String, Object> state, Map<String, Object> slot) {
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(isStepDefinition(item) ? item : resolveValue(item, state, slot));
            }
            return copy;
        }
        if (value instanceof Map<?, ?> map) {
            if (Boolean.TRUE.equals(map.get(OPTIONAL_FLAG))) {
                return resolveValue(map.get("value"), state, slot);
            }
            if (isStepDefinition(map)) {
                return map;
            }
            Map<String, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = String.valueOf(entry.getKey());
                if ("bindings".equals(key)) {
                    copy.put(key, cloneLiteral(entry.getValue()));
                } else {
                    copy.put(key, resolveValue(entry.getValue(), state, slot));
                }
            }
            return copy;
        }
        if (!(value instanceof String str)) {
            return value;
        }
        if ("__lcod_state__".equals(str)) {
            return cloneLiteral(state);
        }
        if ("__lcod_result__".equals(str)) {
            return null;
        }
        if (str.startsWith("$.")) {
            return getByPath(Map.of("$", state), str);
        }
        if (str.startsWith("$slot.")) {
            return getByPath(Map.of("$slot", slot == null ? Map.of() : slot), str);
        }
        return value;
    }

    private static boolean isStepDefinition(Object value) {
        if (!(value instanceof Map<?, ?> map)) return false;
        Object call = map.get("call");
        return call instanceof String && !((String) call).isBlank();
    }

    private static Object getByPath(Map<String, Object> root, String path) {
        if (path == null || !path.contains(".")) return null;
        String[] parts = path.split("\\.");
        Object current = root;
        for (int i = 0; i < parts.length; i++) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = map.get(parts[i]);
            if (current == null) {
                return null;
            }
        }
        return cloneLiteral(current);
    }

    private static Map<String, Object> asObject(Object value) {
        if (value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> casted = (Map<String, Object>) map;
            return casted;
        }
        return null;
    }

    private static Object cloneLiteral(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                copy.put(String.valueOf(entry.getKey()), cloneLiteral(entry.getValue()));
            }
            return copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(cloneLiteral(item));
            }
            return copy;
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static void applyOutputs(Map<String, Object> step, Map<String, Object> state, Object result) {
        Object spreads = Optional.ofNullable(castMap(step.get("out"))).map(out -> out.get(SPREAD_KEY)).orElse(null);
        if (spreads instanceof List<?> descriptors && result instanceof Map<?, ?> resMap) {
            for (Object descriptorObj : descriptors) {
                if (!(descriptorObj instanceof Map<?, ?> descriptor)) continue;
                Object source = descriptor.get("source");
                Object payload = switch (source) {
                    case String str when "$".equals(str) -> result;
                    case String str when "__lcod_result__".equals(str) -> result;
                    case String str when str.startsWith("$.") -> getByPath(Map.of("$", result), str);
                    default -> result;
                };
                Map<String, Object> payloadMap = asObject(payload);
                if (payloadMap == null) {
                    if (!Boolean.TRUE.equals(descriptor.get("optional"))) {
                        continue;
                    }
                    continue;
                }
                Object pick = descriptor.get("pick");
                if (pick instanceof List<?> pickList) {
                    for (Object keyObj : pickList) {
                        String key = String.valueOf(keyObj);
                        if (payloadMap.containsKey(key)) {
                            state.put(key, cloneLiteral(payloadMap.get(key)));
                        } else if (!Boolean.TRUE.equals(descriptor.get("optional"))) {
                            state.put(key, null);
                        }
                    }
                } else {
                    for (Map.Entry<String, Object> entry : payloadMap.entrySet()) {
                        state.put(entry.getKey(), cloneLiteral(entry.getValue()));
                    }
                }
            }
        }

        Map<String, Object> outs = castMap(step.get("out"));
        for (Map.Entry<String, Object> entry : outs.entrySet()) {
            if (SPREAD_KEY.equals(entry.getKey())) continue;
            Object aliasValue = entry.getValue();
            boolean optional = false;
            if (aliasValue instanceof Map<?, ?> map && Boolean.TRUE.equals(map.get(OPTIONAL_FLAG))) {
                optional = true;
                aliasValue = map.get("value");
            }
            Object resolved;
            if ("$".equals(aliasValue)) {
                resolved = result;
            } else if (aliasValue instanceof String str && result instanceof Map<?, ?> resMap) {
                resolved = ((Map<?, ?>) result).get(str);
            } else {
                resolved = null;
            }
            if (optional && resolved == null) {
                continue;
            }
            state.put(entry.getKey(), resolved);
        }
    }
}
