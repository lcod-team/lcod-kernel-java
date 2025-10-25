package work.lcod.kernel.flow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import work.lcod.kernel.runtime.ComposeLoader;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.support.KernelTestSupport;

/**
 * Port of the foreach scenarios covered by the Node/Rust kernels. These tests ensure
 * slot plumbing, control signals, and collectPath semantics stay in sync across runtimes.
 */
final class FlowForeachTest {
    @Test
    void collectsChildOutputWithCollectPath() throws Exception {
        var ctx = KernelTestSupport.demoContext();
        var compose = List.of(foreachStep(
            Map.of("list", "$.numbers"),
            List.of(echoStep("$slot.item")),
            null,
            "$.val"
        ));

        var result = ComposeRunner.runSteps(ctx, compose, Map.of("numbers", List.of(1, 2, 3)), Map.of());
        assertEquals(List.of(1, 2, 3), result.get("results"));
    }

    @Test
    void executesElseSlotWhenListIsEmpty() throws Exception {
        var ctx = KernelTestSupport.demoContext();
        var compose = List.of(foreachStep(
            Map.of("list", "$.numbers"),
            List.of(echoStep("$slot.item")),
            List.of(echoLiteral("empty")),
            "$.val"
        ));

        var result = ComposeRunner.runSteps(ctx, compose, Map.of("numbers", List.of()), Map.of());
        assertEquals(List.of("empty"), result.get("results"));
    }

    @Test
    void exposesSlotVariablesInCollectPath() throws Exception {
        var ctx = KernelTestSupport.demoContext();
        var compose = List.of(foreachStep(
            Map.of("list", "$.numbers"),
            List.of(),
            null,
            "$slot.index"
        ));

        var result = ComposeRunner.runSteps(ctx, compose, Map.of("numbers", List.of("a", "b", "c")), Map.of());
        assertEquals(List.of(0, 1, 2), result.get("results"));
    }

    @Test
    void handlesContinueAndBreakSignals() throws Exception {
        var ctx = KernelTestSupport.demoContext();
        var compose = List.of(foreachStep(
            Map.of("list", "$.numbers"),
            List.of(
                call("lcod://impl/is_even@1", Map.of("value", "$slot.item"), Map.of("isEven", "ok")),
                flowIf("$.isEven", List.of(call("lcod://flow/continue@1", null, null)), List.of()),
                call("lcod://impl/gt@1", Map.of("value", "$slot.item", "limit", 7), Map.of("tooBig", "ok")),
                flowIf("$.tooBig", List.of(call("lcod://flow/break@1", null, null)), List.of()),
                echoStep("$slot.item")
            ),
            null,
            "$.val"
        ));

        var result = ComposeRunner.runSteps(ctx, compose, Map.of("numbers", List.of(1, 2, 3, 8, 9)), Map.of());
        assertEquals(List.of(1, 3), result.get("results"));
    }

    @Test
    void specForeachCtrlDemoMatchesReferenceOutput() throws Exception {
        Optional<Path> specRoot = KernelTestSupport.locateSpecRepo();
        Assumptions.assumeTrue(specRoot.isPresent(), "SPEC_REPO_PATH or ../lcod-spec is required for this test");

        var compose = ComposeLoader.loadFromLocalFile(specRoot.get().resolve("examples/flow/foreach_ctrl_demo/compose.yaml"));
        var ctx = KernelTestSupport.demoContext();
        var state = ComposeRunner.runSteps(ctx, compose, Map.of("numbers", List.of(1, 2, 3, 8, 9)), Map.of());

        assertEquals(List.of(1, 3), state.get("results"));
    }

    private static Map<String, Object> foreachStep(Map<String, Object> in, List<Map<String, Object>> body, List<Map<String, Object>> elseSteps, String collectPath) {
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://flow/foreach@1");
        step.put("in", in);

        var slots = new LinkedHashMap<String, Object>();
        slots.put("body", body);
        if (elseSteps != null) {
            slots.put("else", elseSteps);
        }
        step.put("slots", slots);
        step.put("collectPath", collectPath);
        step.put("out", Map.of("results", "results"));
        return step;
    }

    private static Map<String, Object> echoStep(Object valueExpression) {
        return call("lcod://impl/echo@1", Map.of("value", valueExpression), Map.of("val", "val"));
    }

    private static Map<String, Object> echoLiteral(Object literal) {
        return call("lcod://impl/echo@1", Map.of("value", literal), Map.of("val", "val"));
    }

    private static Map<String, Object> flowIf(Object condition, List<Map<String, Object>> thenSteps, List<Map<String, Object>> elseSteps) {
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://flow/if@1");
        step.put("in", Map.of("cond", condition));
        var slots = new LinkedHashMap<String, Object>();
        slots.put("then", thenSteps);
        if (elseSteps != null && !elseSteps.isEmpty()) {
            slots.put("else", elseSteps);
        }
        step.put("slots", slots);
        return step;
    }

    private static Map<String, Object> call(String id, Map<String, Object> input, Map<String, Object> out) {
        var step = new LinkedHashMap<String, Object>();
        step.put("call", id);
        if (input != null) {
            step.put("in", input);
        }
        if (out != null) {
            step.put("out", out);
        }
        return step;
    }
}
