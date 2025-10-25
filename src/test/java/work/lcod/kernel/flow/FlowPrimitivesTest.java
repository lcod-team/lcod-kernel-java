package work.lcod.kernel.flow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;

class FlowPrimitivesTest {
    @Test
    void flowIfSelectsBranches() throws Exception {
        var registry = baseRegistry();
        FlowPrimitives.register(registry);
        var ctx = new ExecutionContext(registry);

        var thenStep = stepSet("selected", "then");
        var elseStep = stepSet("selected", "else");

        var flowIf = new LinkedHashMap<String, Object>();
        flowIf.put("call", "lcod://flow/if@1");
        flowIf.put("in", Map.of("cond", true));
        flowIf.put("slots", Map.of(
            "then", List.of(thenStep),
            "else", List.of(elseStep)
        ));
        flowIf.put("out", Map.of("result", "selected"));

        var state = ComposeRunner.runSteps(ctx, List.of(flowIf), new LinkedHashMap<>(), Map.of());
        assertEquals("then", state.get("result"));

        flowIf.put("in", Map.of("cond", false));
        var stateElse = ComposeRunner.runSteps(ctx, List.of(flowIf), new LinkedHashMap<>(), Map.of());
        assertEquals("else", stateElse.get("result"));
    }

    @Test
    void flowForeachCollectsAndHandlesSignals() throws Exception {
        var registry = baseRegistry();
        FlowPrimitives.register(registry);
        registry.register("test://action@1", (ctx, input, meta) -> {
            var action = String.valueOf(input.get("action"));
            if ("continue".equals(action)) throw FlowSignalException.continueSignal();
            if ("break".equals(action)) throw FlowSignalException.breakSignal();
            return Map.of("loopValue", action);
        });
        var ctx = new ExecutionContext(registry);

        var foreach = new LinkedHashMap<String, Object>();
        foreach.put("call", "lcod://flow/foreach@1");
        foreach.put("collectPath", "$.loopValue");
        foreach.put("in", Map.of("list", List.of("keep-1", "continue", "keep-2", "break", "keep-3")));
        foreach.put("slots", Map.of(
            "body", List.of(Map.of(
                "call", "test://action@1",
                "in", Map.of("action", "$slot.item"),
                "out", Map.of("loopValue", "loopValue")
            ))
        ));
        foreach.put("out", Map.of("collected", "results"));

        var state = ComposeRunner.runSteps(ctx, List.of(foreach), new LinkedHashMap<>(), Map.of());
        assertIterableEquals(List.of("keep-1", "keep-2"), (List<?>) state.get("collected"));

        foreach.put("in", Map.of("list", List.of()));
        var stateEmpty = ComposeRunner.runSteps(ctx, List.of(foreach), new LinkedHashMap<>(), Map.of());
        assertEquals(List.of(), stateEmpty.get("collected"));
    }

    @Test
    void flowTryHandlesCatchAndFinally() throws Exception {
        var registry = baseRegistry();
        FlowPrimitives.register(registry);
        var ctx = new ExecutionContext(registry);

        var tryStep = new LinkedHashMap<String, Object>();
        tryStep.put("call", "lcod://flow/try@1");
        tryStep.put("slots", Map.of(
            "children", List.of(Map.of(
                "call", "lcod://flow/throw@1",
                "in", Map.of("message", "boom", "code", "boom_code")
            )),
            "catch", List.of(stepSet("handled", true)),
            "finally", List.of(stepSet("cleanup", "done"))
        ));
        tryStep.put("out", Map.of(
            "handled", "handled",
            "cleanup", "cleanup"
        ));

        var state = ComposeRunner.runSteps(ctx, List.of(tryStep), new LinkedHashMap<>(), Map.of());
        assertEquals(true, state.get("handled"));
        assertEquals("done", state.get("cleanup"));

        tryStep.put("slots", Map.of(
            "children", List.of(Map.of(
                "call", "lcod://flow/throw@1",
                "in", Map.of("message", "boom", "code", "boom_code")
            ))
        ));

        var thrown = assertThrows(FlowErrorException.class, () ->
            ComposeRunner.runSteps(ctx, List.of(tryStep), new LinkedHashMap<>(), Map.of())
        );
        assertEquals("boom_code", thrown.code());
        assertEquals("boom", thrown.getMessage());
    }

    @Test
    void flowParallelCollectsResults() throws Exception {
        var registry = baseRegistry();
        FlowPrimitives.register(registry);
        var ctx = new ExecutionContext(registry);

        var parallel = new LinkedHashMap<String, Object>();
        parallel.put("call", "lcod://flow/parallel@1");
        parallel.put("collectPath", "$.result");
        parallel.put("in", Map.of("tasks", List.of("a", "b", "c")));
        parallel.put("slots", Map.of(
            "tasks", List.of(Map.of(
                "call", "lcod://impl/set@1",
                "in", Map.of("result", "$slot.item"),
                "out", Map.of("result", "result")
            ))
        ));
        parallel.put("out", Map.of("joined", "results"));

        var state = ComposeRunner.runSteps(ctx, List.of(parallel), new LinkedHashMap<>(), Map.of());
        assertIterableEquals(List.of("a", "b", "c"), (List<?>) state.get("joined"));
    }

    @Test
    void flowCheckAbortPassThrough() throws Exception {
        var registry = baseRegistry();
        FlowPrimitives.register(registry);
        var ctx = new ExecutionContext(registry);

        var check = new LinkedHashMap<String, Object>();
        check.put("call", "lcod://flow/check_abort@1");
        var state = ComposeRunner.runSteps(ctx, List.of(check), new LinkedHashMap<>(), Map.of());
        assertEquals(0, state.size());
    }

    @Test
    void flowWhileIteratesAndRunsElse() throws Exception {
        var registry = baseRegistry();
        FlowPrimitives.register(registry);
        registry.register("test://while/condition", (ctx, input, meta) -> {
            var count = ((Number) input.getOrDefault("count", 0)).intValue();
            var limit = ((Number) input.getOrDefault("limit", 0)).intValue();
            return Map.of("continue", count < limit);
        });
        registry.register("test://while/body", (ctx, input, meta) -> {
            var map = new LinkedHashMap<>(input);
            var count = ((Number) map.getOrDefault("count", 0)).intValue();
            map.put("count", count + 1);
            return map;
        });
        registry.register("test://while/else", (ctx, input, meta) -> Map.of("count", 999));
        var ctx = new ExecutionContext(registry);

        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://flow/while@1");
        step.put("in", Map.of("state", Map.of("count", 0, "limit", 3)));
        step.put("slots", Map.of(
            "condition", List.of(Map.of("call", "test://while/condition")),
            "body", List.of(Map.of("call", "test://while/body")),
            "else", List.of(Map.of("call", "test://while/else"))
        ));
        step.put("out", Map.of("final", "state", "iters", "iterations"));

        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals(3, ((Map<?, ?>) state.get("final")).get("count"));
        assertEquals(3, state.get("iters"));

        // else branch
        step.put("in", Map.of("state", Map.of("count", 5, "limit", 5)));
        var elseState = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals(999, ((Map<?, ?>) elseState.get("final")).get("count"));
    }

    @Test
    void flowWhileRespectsMaxIterations() throws Exception {
        var registry = baseRegistry();
        FlowPrimitives.register(registry);
        registry.register("test://while/always", (ctx, input, meta) -> Map.of("continue", true));
        registry.register("test://while/nop", (ctx, input, meta) -> input);
        var ctx = new ExecutionContext(registry);

        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://flow/while@1");
        step.put("in", Map.of("state", Map.of("count", 0), "maxIterations", 1));
        step.put("slots", Map.of(
            "condition", List.of(Map.of("call", "test://while/always")),
            "body", List.of(Map.of("call", "test://while/nop"))
        ));

        assertThrows(FlowErrorException.class, () ->
            ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of())
        );
    }

    private Registry baseRegistry() {
        var registry = new Registry();
        registry.register("lcod://impl/set@1", (ctx, input, meta) -> new LinkedHashMap<>(input));
        registry.register("lcod://kernel/log@1", (ctx, input, meta) -> Map.of());
        return registry;
    }

    private Map<String, Object> stepSet(String alias, Object value) {
        return Map.of(
            "call", "lcod://impl/set@1",
            "in", Map.of(alias, value),
            "out", Map.of(alias, alias)
        );
    }
}
