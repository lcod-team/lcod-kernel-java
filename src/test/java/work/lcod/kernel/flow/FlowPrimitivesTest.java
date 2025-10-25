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
