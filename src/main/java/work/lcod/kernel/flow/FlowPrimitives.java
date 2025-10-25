package work.lcod.kernel.flow;

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
 * Registers the core flow operators implemented by the Java kernel.
 */
public final class FlowPrimitives {
    private FlowPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://flow/if@1", FlowPrimitives::flowIf);
        registry.register("lcod://flow/foreach@1", FlowPrimitives::flowForeach);
        registry.register("lcod://flow/continue@1", FlowPrimitives::flowContinue);
        registry.register("lcod://flow/break@1", FlowPrimitives::flowBreak);
        return registry;
    }

    private static Object flowIf(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) throws Exception {
        var cond = input != null && Boolean.TRUE.equals(input.get("cond"));
        var branch = cond ? "then" : "else";
        var branchState = ctx.runSlot(branch, null, null);
        return branchState == null ? Map.of() : branchState;
    }

    private static Object flowForeach(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) throws Exception {
        var source = input != null && input.containsKey("list")
            ? input.get("list")
            : input == null ? null : input.get("stream");
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
}
