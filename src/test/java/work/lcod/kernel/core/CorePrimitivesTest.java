package work.lcod.kernel.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
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

    @Test
    void stringSplit() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/string/split@1");
        step.put("in", Map.of(
            "text", "a, b, ,c",
            "separator", ",",
            "trim", true,
            "removeEmpty", true
        ));
        step.put("out", Map.of("segments", "segments"));

        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        @SuppressWarnings("unchecked")
        var segments = (List<String>) state.get("segments");
        assertEquals(List.of("a", "b", "c"), segments);
    }

    @Test
    void stringTrim() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/string/trim@1");
        step.put("in", Map.of("text", "  hi  "));
        step.put("out", Map.of("trimmed", "value"));
        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals("hi", state.get("trimmed"));
    }

    @Test
    void arrayShiftReturnsHeadAndRest() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/array/shift@1");
        step.put("in", Map.of("items", List.of("a", "b", "c")));
        step.put("out", Map.of("head", "head", "rest", "rest"));
        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals("a", state.get("head"));
        assertEquals(List.of("b", "c"), state.get("rest"));

        step.put("in", Map.of("items", List.of()));
        var empty = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals(null, empty.get("head"));
        assertEquals(List.of(), empty.get("rest"));
    }

    @Test
    void pathDirnameReturnsParent() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/path/dirname@1");
        step.put("in", Map.of("path", "/tmp/work/file.txt"));
        step.put("out", Map.of("dir", "dirname"));
        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals("/tmp/work", state.get("dir"));

        step.put("in", Map.of("path", "README.md"));
        var relative = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals(".", relative.get("dir"));
    }

    @Test
    void objectEntries() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/object/entries@1");
        step.put("in", Map.of("object", Map.of("foo", 1, "bar", "x")));
        step.put("out", Map.of("pairs", "entries"));
        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        @SuppressWarnings("unchecked")
        var pairs = (List<List<Object>>) state.get("pairs");
        assertEquals(2, pairs.size());
    }

    @Test
    void valueKindReportsKinds() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/value/kind@1");
        step.put("in", Map.of());
        step.put("out", Map.of("kind", "kind"));
        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals("null", state.get("kind"));

        step.put("in", Map.of("value", List.of(1, 2)));
        var arrState = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals("array", arrState.get("kind"));
    }

    @Test
    void numberTruncatesTowardZero() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/number/trunc@1");
        step.put("in", Map.of("value", 3.8));
        step.put("out", Map.of("result", "value"));
        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals(3L, state.get("result"));

        step.put("in", Map.of("value", -4.2));
        var negState = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals(-4L, negState.get("result"));
    }

    @Test
    void valueEqualsComparesDeepValues() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/value/equals@1");
        step.put("in", Map.of(
            "left", Map.of("a", List.of(1, 2)),
            "right", Map.of("a", List.of(1, 2))
        ));
        step.put("out", Map.of("equal", "equal"));
        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        assertEquals(true, state.get("equal"));
    }

    @Test
    void valueCloneProducesIndependentCopy() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);
        var originalList = new ArrayList<Integer>(List.of(1, 2, 3));
        var original = new LinkedHashMap<String, Object>();
        original.put("nested", originalList);

        var step = new LinkedHashMap<String, Object>();
        step.put("call", "lcod://contract/core/value/clone@1");
        step.put("in", Map.of("value", original));
        step.put("out", Map.of("clone", "value"));

        var state = ComposeRunner.runSteps(ctx, List.of(step), new LinkedHashMap<>(), Map.of());
        @SuppressWarnings("unchecked")
        var cloned = (Map<String, Object>) state.get("clone");
        assertEquals(List.of(1, 2, 3), cloned.get("nested"));

        @SuppressWarnings("unchecked")
        var clonedList = (List<Integer>) cloned.get("nested");
        clonedList.set(0, 42);
        assertEquals(List.of(1, 2, 3), original.get("nested"));
    }

    private Registry baseRegistry() {
        var registry = new Registry();
        registry.register("lcod://impl/set@1", (ctx, input, meta) -> Map.copyOf(input));
        CorePrimitives.register(registry);
        return registry;
    }
}
