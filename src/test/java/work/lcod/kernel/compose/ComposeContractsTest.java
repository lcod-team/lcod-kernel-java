package work.lcod.kernel.compose;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;

class ComposeContractsTest {
    @Test
    void runSlotInvokesSlotAndReturnsResult() throws Exception {
        var registry = baseRegistry();
        ComposeContracts.register(registry);
        var ctx = new ExecutionContext(registry);

        var runSlotStep = new LinkedHashMap<String, Object>();
        runSlotStep.put("call", "lcod://contract/compose/run_slot@1");
        runSlotStep.put("in", Map.of("slot", "target"));
        runSlotStep.put("out", Map.of(
            "slotOutput", "result",
            "slotRan", "ran"
        ));
        runSlotStep.put("slots", Map.of(
            "target", List.of(stepSet("value", 123))
        ));

        var state = ComposeRunner.runSteps(ctx, List.of(runSlotStep), new LinkedHashMap<>(), Map.of());
        assertTrue(Boolean.TRUE.equals(state.get("slotRan")));
        @SuppressWarnings("unchecked")
        var payload = (Map<String, Object>) state.get("slotOutput");
        assertEquals(123, payload.get("value"));
    }

    @Test
    void optionalSkipMissingSlot() throws Exception {
        var registry = baseRegistry();
        ComposeContracts.register(registry);
        var ctx = new ExecutionContext(registry);

        var runSlotStep = new LinkedHashMap<String, Object>();
        runSlotStep.put("call", "lcod://contract/compose/run_slot@1");
        runSlotStep.put("in", Map.of("slot", "missing", "optional", true));
        runSlotStep.put("out", Map.of(
            "slotOutput", "result",
            "slotRan", "ran"
        ));

        var state = ComposeRunner.runSteps(ctx, List.of(runSlotStep), new LinkedHashMap<>(), Map.of());
        assertFalse(Boolean.TRUE.equals(state.get("slotRan")));
        assertEquals(null, state.get("slotOutput"));
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
