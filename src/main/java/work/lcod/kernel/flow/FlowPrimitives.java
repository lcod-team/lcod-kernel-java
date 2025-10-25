package work.lcod.kernel.flow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;

/**
 * Mirrors the flow helpers available in the JS/Rust kernels.
 */
public final class FlowPrimitives {
    private static final ObjectMapper CLONER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_REF = new TypeReference<>() {};

    private FlowPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://flow/if@1", FlowPrimitives::flowIf);
        registry.register("lcod://flow/foreach@1", FlowPrimitives::flowForeach);
        registry.register("lcod://flow/continue@1", FlowPrimitives::flowContinue);
        registry.register("lcod://flow/break@1", FlowPrimitives::flowBreak);
        registry.register("lcod://flow/throw@1", FlowPrimitives::flowThrow);
        registry.register("lcod://flow/try@1", FlowPrimitives::flowTry);
        registry.register("lcod://flow/parallel@1", FlowPrimitives::flowParallel);
        registry.register("lcod://flow/check_abort@1", FlowPrimitives::flowCheckAbort);
        registry.register("lcod://flow/while@1", FlowPrimitives::flowWhile);
        return registry;
    }

    private static Object flowIf(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) throws Exception {
        var cond = input != null && Boolean.TRUE.equals(input.get("cond"));
        var branch = cond ? "then" : "else";
        var branchState = ctx.runSlot(branch, null, null);
        return branchState == null ? Map.of() : branchState;
    }

    private static Object flowForeach(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) throws Exception {
        var source = input != null && input.containsKey("list") ? input.get("list") : input == null ? null : input.get("stream");
        var items = toList(source);
        var results = new ArrayList<>();
        if (items.isEmpty()) {
            var elseVars = slotVars(null, -1);
            var elseState = ctx.runSlot("else", null, elseVars);
            collect(meta, results, elseState, elseVars, null, false);
            return Map.of("results", results);
        }
        for (var index = 0; index < items.size(); index++) {
            ctx.ensureNotCancelled();
            var item = items.get(index);
            var slotVars = slotVars(item, index);
            try {
                var iterState = ctx.runSlot("body", null, slotVars);
                collect(meta, results, iterState, slotVars, item, true);
            } catch (FlowSignalException signal) {
                if (signal.signal() == FlowSignal.CONTINUE) {
                    continue;
                }
                if (signal.signal() == FlowSignal.BREAK) {
                    break;
                }
                throw signal;
            }
        }
        return Map.of("results", results);
    }

    private static Object flowContinue(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) {
        throw FlowSignalException.continueSignal();
    }

    private static Object flowBreak(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) {
        throw FlowSignalException.breakSignal();
    }

    private static Object flowThrow(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) {
        var message = input != null && input.get("message") instanceof String s && !s.isBlank() ? s : "Flow throw";
        var code = input != null && input.get("code") instanceof String s && !s.isBlank() ? s : "flow_throw";
        var data = input == null ? null : input.get("data");
        throw new FlowErrorException(code, message, data);
    }

    private static Object flowTry(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) throws Exception {
        Map<String, Object> resultState = new LinkedHashMap<>();
        Map<String, Object> pendingError = null;
        try {
            var tryVars = Map.<String, Object>of("phase", "try");
            var runState = ctx.runSlot("children", null, tryVars);
            if (runState != null) {
                resultState.putAll(runState);
            }
        } catch (Throwable err) {
            pendingError = FlowErrorUtils.normalize(err);
            if (hasSlot(meta, "catch")) {
                try {
                    var catchState = ctx.runSlot("catch", null, phaseMap("catch", pendingError));
                    resultState.clear();
                    if (catchState != null) {
                        resultState.putAll(catchState);
                    }
                    pendingError = null;
                } catch (Throwable catchErr) {
                    pendingError = FlowErrorUtils.normalize(catchErr);
                }
            }
        } finally {
            if (hasSlot(meta, "finally")) {
                var finallyState = ctx.runSlot("finally", null, phaseMap("finally", pendingError));
                if (finallyState != null) {
                    resultState.putAll(finallyState);
                }
            }
        }

        if (pendingError != null) {
            var code = String.valueOf(pendingError.getOrDefault("code", "unexpected_error"));
            var message = String.valueOf(pendingError.getOrDefault("message", "Unexpected error"));
            var data = pendingError.get("data");
            throw new FlowErrorException(code, message, data);
        }

        return resultState;
    }

    private static Object flowParallel(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) throws Exception {
        var tasks = input != null && input.get("tasks") instanceof List<?> list ? list : List.of();
        if (!hasSlot(meta, "tasks")) {
            return Map.of("results", List.of());
        }
        var collectPath = meta == null ? null : meta.collectPath();
        var results = new ArrayList<>();
        for (var index = 0; index < tasks.size(); index++) {
            ctx.ensureNotCancelled();
            var slotVars = slotVars(tasks.get(index), index);
            try {
                var iterState = ctx.runSlot("tasks", null, slotVars);
                if (collectPath != null && !collectPath.isBlank()) {
                    var value = getByPath(Map.of("$", iterState == null ? Map.of() : iterState, "$slot", slotVars), collectPath);
                    results.add(value);
                } else {
                    results.add(iterState);
                }
            } catch (Throwable err) {
                var normalized = FlowErrorUtils.normalize(err);
                var code = String.valueOf(normalized.getOrDefault("code", "unexpected_error"));
                var message = String.valueOf(normalized.getOrDefault("message", "Unexpected error"));
                var data = normalized.get("data");
                throw new FlowErrorException(code, message, data);
            }
        }
        return Map.of("results", results);
    }

    private static Object flowCheckAbort(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) {
        ctx.ensureNotCancelled();
        return Map.of();
    }

    private static Object flowWhile(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) throws Exception {
        var state = normalizeState(input == null ? null : input.get("state"));
        var maxIterations = readMaxIterations(input == null ? null : input.get("maxIterations"));
        var iterations = 0;

        while (true) {
            ctx.ensureNotCancelled();
            if (maxIterations != null && iterations >= maxIterations) {
                throw new FlowErrorException("flow_while_max_iterations", "flow/while exceeded maxIterations (" + maxIterations + ")", null);
            }
            var slotVars = new LinkedHashMap<String, Object>();
            slotVars.put("index", iterations);
            slotVars.put("state", deepClone(state));

            var conditionOutput = ctx.runSlot("condition", deepClone(state), slotVars);
            ctx.ensureNotCancelled();
            var interpreted = interpretCondition(conditionOutput);
            if (interpreted.stateOverride != null) {
                state = interpreted.stateOverride;
            }
            if (!interpreted.shouldContinue) {
                break;
            }

            try {
                var bodyResult = ctx.runSlot("body", deepClone(state), slotVars);
                ctx.ensureNotCancelled();
                if (bodyResult == null) {
                    // keep state
                } else if (bodyResult instanceof Map<?, ?> map) {
                    state = deepClone(map);
                } else {
                    throw new FlowErrorException("flow_while_invalid_body", "flow/while body must return an object or null", Map.of("type", bodyResult.getClass().getSimpleName()));
                }
            } catch (FlowSignalException signal) {
                if (signal.signal() == FlowSignal.CONTINUE) {
                    iterations += 1;
                    continue;
                }
                if (signal.signal() == FlowSignal.BREAK) {
                    iterations += 1;
                    break;
                }
                throw signal;
            }

            iterations += 1;
        }

        if (iterations == 0 && hasSlot(meta, "else")) {
            var elseVars = new LinkedHashMap<String, Object>();
            elseVars.put("index", -1);
            elseVars.put("state", deepClone(state));
            var elseResult = ctx.runSlot("else", deepClone(state), elseVars);
            if (elseResult == null) {
                // keep state
            } else if (elseResult instanceof Map<?, ?> map) {
                state = deepClone(map);
            } else {
                throw new FlowErrorException("flow_while_invalid_else", "flow/while else must return an object or null", Map.of("type", elseResult.getClass().getSimpleName()));
            }
        }

        var result = new LinkedHashMap<String, Object>();
        result.put("state", deepClone(state));
        result.put("iterations", iterations);
        return result;
    }

    private static void collect(work.lcod.kernel.runtime.StepMeta meta, List<Object> target, Map<String, Object> iterState, Map<String, Object> slotVars, Object fallback, boolean useFallback) {
        var collectPath = meta == null ? null : meta.collectPath();
        if (collectPath == null || collectPath.isBlank()) {
            if (useFallback) {
                target.add(fallback);
            }
            return;
        }
        var root = new HashMap<String, Object>();
        root.put("$", iterState == null ? Map.of() : iterState);
        root.put("$slot", slotVars == null ? Map.of() : slotVars);
        var value = getByPath(root, collectPath);
        if (value != null) {
            target.add(value);
        }
    }

    private static Object getByPath(Map<String, Object> root, String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        var segments = path.split("\\.");
        Object current = root;
        for (var segment : segments) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = map.get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static List<Object> toList(Object source) {
        if (source == null) {
            return Collections.emptyList();
        }
        if (source instanceof List<?> list) {
            return new ArrayList<>(list);
        }
        if (source instanceof Iterable<?> iterable) {
            var collected = new ArrayList<>();
            for (var item : iterable) {
                collected.add(item);
            }
            return collected;
        }
        if (source instanceof Stream<?> stream) {
            return stream.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }
        if (source.getClass().isArray()) {
            var length = Array.getLength(source);
            var collected = new ArrayList<>();
            for (var i = 0; i < length; i++) {
                collected.add(Array.get(source, i));
            }
            return collected;
        }
        if (source instanceof Iterator<?> iterator) {
            var collected = new ArrayList<>();
            iterator.forEachRemaining(collected::add);
            return collected;
        }
        return Collections.singletonList(source);
    }

    private static Map<String, Object> slotVars(Object item, int index) {
        var vars = new LinkedHashMap<String, Object>();
        vars.put("item", item);
        vars.put("index", index);
        return vars;
    }

    private static boolean hasSlot(work.lcod.kernel.runtime.StepMeta meta, String name) {
        if (meta == null || meta.slots() == null) {
            return false;
        }
        var entries = meta.slots().get(name);
        return entries != null && !entries.isEmpty();
    }

    private static Map<String, Object> phaseMap(String phase, Map<String, Object> error) {
        var vars = new LinkedHashMap<String, Object>();
        vars.put("phase", phase);
        if (error != null) {
            vars.put("error", error);
        }
        return vars;
    }

    private static Map<String, Object> deepClone(Object value) {
        if (value == null) {
            return new LinkedHashMap<>();
        }
        if (value instanceof Map<?, ?> map) {
            return CLONER.convertValue(map, MAP_REF);
        }
        throw new FlowErrorException("flow_while_invalid_state", "flow/while state must be an object", Map.of("type", value.getClass().getSimpleName()));
    }

    private static Map<String, Object> normalizeState(Object raw) {
        if (raw == null) {
            return new LinkedHashMap<>();
        }
        if (raw instanceof Map<?, ?> map) {
            return deepClone(map);
        }
        throw new FlowErrorException("flow_while_invalid_state", "flow/while state must be an object", Map.of("type", raw.getClass().getSimpleName()));
    }

    private static Integer readMaxIterations(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof Number number) {
            var value = number.intValue();
            if (value <= 0) {
                return null;
            }
            if (value != number.doubleValue()) {
                throw new FlowErrorException("flow_while_invalid_max_iterations", "maxIterations must be an integer", Map.of("value", raw));
            }
            return value;
        }
        throw new FlowErrorException("flow_while_invalid_max_iterations", "maxIterations must be numeric", Map.of("value", raw));
    }

    private static Condition interpretCondition(Object output) {
        if (output == null) {
            return new Condition(false, null);
        }
        if (output instanceof Map<?, ?> map) {
            Map<String, Object> stateOverride = null;
            if (map.get("state") instanceof Map<?, ?> stateMap) {
                stateOverride = deepClone(stateMap);
            }
            Object candidate = map.containsKey("continue") ? map.get("continue") : map.containsKey("cond") ? map.get("cond") : map.get("value");
            if (candidate != null) {
                return new Condition(isTruthy(candidate), stateOverride);
            }
            if (map.isEmpty()) {
                return new Condition(false, stateOverride);
            }
            throw new FlowErrorException("flow_while_invalid_condition", "condition slot must return a boolean or object with cond/value", Map.of());
        }
        return new Condition(isTruthy(output), null);
    }

    private static boolean isTruthy(Object value) {
        if (value == null) return false;
        if (value instanceof Boolean b) return b;
        if (value instanceof Number n) return n.doubleValue() != 0 && !Double.isNaN(n.doubleValue());
        if (value instanceof String s) return !s.isEmpty();
        if (value instanceof List<?> list) return !list.isEmpty();
        if (value instanceof Map<?, ?> map) return !map.isEmpty();
        return true;
    }

    private record Condition(boolean shouldContinue, Map<String, Object> stateOverride) {}
}
