package work.lcod.kernel.runtime;

import java.util.ArrayList;
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
        var state = initialState == null ? new LinkedHashMap<String, Object>() : new LinkedHashMap<>(initialState);
        var steps = rawSteps == null ? List.<Map<String, Object>>of() : rawSteps;

        for (int index = 0; index < steps.size(); index++) {
            ctx.ensureNotCancelled();
            var step = steps.get(index);
            if (step == null) continue;
            var slotMap = normalizeSlotMap(step);
            var childrenMeta = slotMap == null ? Map.<String, List<Map<String, Object>>>of() : new LinkedHashMap<>(slotMap);
            if (childrenMeta.containsKey("body") && !childrenMeta.containsKey("children")) {
                childrenMeta.put("children", childrenMeta.get("body"));
            }

            var previousChildren = ctx.childRunner();
            var previousSlotRunner = ctx.slotRunner();
            ctx.setChildRunner((children, localState, slotOverrides) -> {
                ctx.ensureNotCancelled();
                var base = localState == null ? state : localState;
                ctx.pushScope();
                try {
                    return runSteps(ctx, safeSteps(children), base, slotOverrides == null ? slotVars : slotOverrides);
                } finally {
                    ctx.popScope();
                }
            });
            ctx.setSlotRunner((name, localState, slotOverrides) -> {
                ctx.ensureNotCancelled();
                if (slotMap == null && previousSlotRunner != null) {
                    return previousSlotRunner.runSlot(name, localState, slotOverrides);
                }
                var target = resolveSlotSteps(slotMap, name);
                var base = localState == null ? state : localState;
                ctx.pushScope();
                try {
                    return runSteps(ctx, target, base, slotOverrides == null ? slotVars : slotOverrides);
                } finally {
                    ctx.popScope();
                }
            });

            var input = buildInput(castMap(step.get("in")), state, slotVars);
            Object result;
            try {
                ctx.pushScope();
                var callId = Objects.toString(step.get("call"), null);
                boolean isScriptCall = "lcod://tooling/script@1".equals(callId);
                if (isScriptCall) {
                    ctx.setAttribute("__lcod_state__", cloneLiteral(state));
                }
                result = ctx.call(callId, input, new StepMeta(childrenMeta, slotVars, Objects.toString(step.get("collectPath"), null)));
            } finally {
                ctx.setAttribute("__lcod_state__", null);
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
        var result = new LinkedHashMap<String, Object>();
        var spreads = bindings.get(SPREAD_KEY);
        if (spreads instanceof List<?> descriptors) {
            for (var descriptorObj : descriptors) {
                if (!(descriptorObj instanceof Map<?, ?> descriptor)) continue;
                var sourceNode = resolveValue(descriptor.get("source"), state, slot);
                var payload = asObject(sourceNode);
                if (payload == null) {
                    if (!Boolean.TRUE.equals(descriptor.get("optional"))) {
                        continue;
                    }
                    continue;
                }
                var pick = descriptor.get("pick");
                if (pick instanceof List<?> pickList) {
                    for (var keyObj : pickList) {
                        var key = String.valueOf(keyObj);
                        if (payload.containsKey(key)) {
                            result.put(key, cloneLiteral(payload.get(key)));
                        } else if (!Boolean.TRUE.equals(descriptor.get("optional"))) {
                            result.put(key, null);
                        }
                    }
                } else {
                    for (var entry : payload.entrySet()) {
                        result.put(entry.getKey(), cloneLiteral(entry.getValue()));
                    }
                }
            }
        }

        for (var entry : bindings.entrySet()) {
            if (SPREAD_KEY.equals(entry.getKey())) continue;
            var value = entry.getValue();
            if ("bindings".equals(entry.getKey())) {
                result.put("bindings", cloneLiteral(value));
                continue;
            }
            var optional = false;
            if (value instanceof Map<?, ?> map && Boolean.TRUE.equals(map.get(OPTIONAL_FLAG))) {
                optional = true;
                value = map.get("value");
            }
            var resolved = resolveValue(value, state, slot);
            if (optional && (resolved == null)) {
                continue;
            }
            result.put(entry.getKey(), resolved);
        }
        return result;
    }

    private static Object resolveValue(Object value, Map<String, Object> state, Map<String, Object> slot) {
        if (value instanceof List<?> list) {
            var copy = new ArrayList<>(list.size());
            for (var item : list) {
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
            var copy = new LinkedHashMap<String, Object>();
            for (var entry : map.entrySet()) {
                var key = String.valueOf(entry.getKey());
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
        var parts = path.split("\\.");
        Object current = root;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (current instanceof Map<?, ?> map) {
                current = map.get(part);
            } else if (current instanceof List<?> list) {
                int index = parseIndex(part);
                if (index < 0 || index >= list.size()) {
                    return null;
                }
                current = list.get(index);
            } else {
                return null;
            }
            if (current == null) {
                return null;
            }
        }
        return cloneLiteral(current);
    }

    private static int parseIndex(String token) {
        try {
            return Integer.parseInt(token);
        } catch (NumberFormatException ex) {
            return -1;
        }
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
            var copy = new LinkedHashMap<String, Object>();
            for (var entry : map.entrySet()) {
                copy.put(String.valueOf(entry.getKey()), cloneLiteral(entry.getValue()));
            }
            return copy;
        }
        if (value instanceof List<?> list) {
            var copy = new ArrayList<>(list.size());
            for (var item : list) {
                copy.add(cloneLiteral(item));
            }
            return copy;
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static void applyOutputs(Map<String, Object> step, Map<String, Object> state, Object result) {
        var spreads = Optional.ofNullable(castMap(step.get("out"))).map(out -> out.get(SPREAD_KEY)).orElse(null);
        if (spreads instanceof List<?> descriptors && result instanceof Map<?, ?> resMap) {
            for (var descriptorObj : descriptors) {
                if (!(descriptorObj instanceof Map<?, ?> descriptor)) continue;
                var source = descriptor.get("source");
                Object payload = result;
                if (source instanceof String str) {
                    if ("$".equals(str) || "__lcod_result__".equals(str)) {
                        payload = result;
                    } else if (str.startsWith("$.")) {
                        payload = getByPath(Map.of("$", result), str);
                    }
                }
                var payloadMap = asObject(payload);
                if (payloadMap == null) {
                    if (!Boolean.TRUE.equals(descriptor.get("optional"))) {
                        continue;
                    }
                    continue;
                }
                var pick = descriptor.get("pick");
                if (pick instanceof List<?> pickList) {
                    for (var keyObj : pickList) {
                        var key = String.valueOf(keyObj);
                        if (payloadMap.containsKey(key)) {
                            state.put(key, cloneLiteral(payloadMap.get(key)));
                        } else if (!Boolean.TRUE.equals(descriptor.get("optional"))) {
                            state.put(key, null);
                        }
                    }
                } else {
                    for (var entry : payloadMap.entrySet()) {
                        state.put(entry.getKey(), cloneLiteral(entry.getValue()));
                    }
                }
            }
        }

        var outs = castMap(step.get("out"));
        for (var entry : outs.entrySet()) {
            if (SPREAD_KEY.equals(entry.getKey())) continue;
            var aliasValue = entry.getValue();
            var optional = false;
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
