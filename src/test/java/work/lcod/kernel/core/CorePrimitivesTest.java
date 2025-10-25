package work.lcod.kernel.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;

class CorePrimitivesTest {
    @Test
    void objectMergeDeep() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://core/object/merge@1");
        step.put("in", Map.of(
            "left", Map.of("foo", Map.of("bar", 1), "arr", List.of(1)),
            "right", Map.of("foo", Map.of("baz", 2), "arr", List.of(2)),
            "deep", true,
            "arrayStrategy", "concat"
        ));
        step.put("out", Map.of("result", "value"));

        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        @SuppressWarnings("unchecked")
        var result = (Map<String, Object>) state.get("result");
        @SuppressWarnings("unchecked")
        var foo = (Map<String, Object>) result.get("foo");
        assertEquals(2, foo.get("baz"));
        assertEquals(List.of(1, 2), result.get("arr"));
    }

    @Test
    void stringFormat() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://core/string/format@1");
        step.put("in", Map.of(
            "template", "Hello {name}",
            "values", Map.of("name", "LCOD")
        ));
        step.put("out", Map.of("text", "value"));

        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals("Hello LCOD", state.get("text"));
    }

    private Registry baseRegistry() {
        var registry = new Registry();
        registry.register("lcod://impl/set@1", (ctx, input, meta) -> Map.copyOf(input));
        CorePrimitives.register(registry);
        return registry;
    }
}
