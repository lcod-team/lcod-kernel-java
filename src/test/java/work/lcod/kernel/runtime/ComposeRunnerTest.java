package work.lcod.kernel.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComposeRunnerTest {
    @Test
    void runsSimpleStepAndStoresOutput() throws Exception {
        Registry registry = new Registry();
        registry.register("demo.echo", (ctx, input, meta) -> Map.of("value", input.get("value")));
        ExecutionContext ctx = new ExecutionContext(registry);

        Map<String, Object> step = new LinkedHashMap<>();
        step.put("call", "demo.echo");
        step.put("in", Map.of("value", 42));
        step.put("out", Map.of("answer", "value"));

        Map<String, Object> finalState = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals(42, finalState.get("answer"));
    }

    @Test
    void resolvesStatePathsInInputs() throws Exception {
        Registry registry = new Registry();
        registry.register("demo.set", (ctx, input, meta) -> Map.of("value", input.get("value")));
        ExecutionContext ctx = new ExecutionContext(registry);

        Map<String, Object> firstStep = new LinkedHashMap<>();
        firstStep.put("call", "demo.set");
        firstStep.put("in", Map.of("value", 7));
        firstStep.put("out", Map.of("count", "value"));

        Map<String, Object> secondStep = new LinkedHashMap<>();
        secondStep.put("call", "demo.set");
        secondStep.put("in", Map.of("value", "$.count"));
        secondStep.put("out", Map.of("copy", "value"));

        Map<String, Object> finalState = ComposeRunner.runSteps(
            ctx,
            List.of(firstStep, secondStep),
            new LinkedHashMap<>(),
            Map.of()
        );
        assertEquals(7, finalState.get("copy"));
        assertTrue(finalState.containsKey("count"));
    }
}
