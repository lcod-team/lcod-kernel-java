package work.lcod.kernel.tooling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import work.lcod.kernel.core.CorePrimitives;
import work.lcod.kernel.demo.DemoPrimitives;
import work.lcod.kernel.flow.FlowPrimitives;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;

final class ToolingPrimitivesTest {
    @Test
    void testCheckerWithInlineCompose() throws Exception {
        var registry = baseRegistry();
        var ctx = new ExecutionContext(registry);

        var compose = List.of(Map.<String, Object>of(
            "call", "lcod://tooling/test_checker@1",
            "in", Map.of(
                "compose", List.of(Map.of(
                    "call", "lcod://impl/echo@1",
                    "in", Map.of("value", "hello"),
                    "out", Map.of("value", "value")
                )),
                "input", Map.of(),
                "expected", Map.of("value", "hello")
            ),
            "out", Map.of("report", "$")
        ));

        var state = ComposeRunner.runSteps(ctx, compose, new LinkedHashMap<>(), Map.of());
        var report = (Map<?, ?>) state.get("report");
        assertTrue(Boolean.TRUE.equals(report.get("success")));
    }

    @Test
    void testCheckerWithComposeRef() throws Exception {
        Path tempDir = Files.createTempDirectory("lcod-test");
        try {
            Path composeFile = tempDir.resolve("demo.yaml");
            Files.writeString(composeFile, "compose:\n  - call: lcod://impl/set@1\n    in:\n      result: 42\n    out:\n      result: result\n");

            var registry = baseRegistry();
            var ctx = new ExecutionContext(registry, tempDir);
            var compose = List.of(Map.<String, Object>of(
                "call", "lcod://tooling/test_checker@1",
                "in", Map.of(
                    "composeRef", Map.of("path", "demo.yaml"),
                    "input", Map.of(),
                    "expected", Map.of("result", 42)
                ),
                "out", Map.of("report", "$")
            ));

            var state = ComposeRunner.runSteps(ctx, compose, new LinkedHashMap<>(), Map.of());
            var report = (Map<?, ?>) state.get("report");
            assertTrue(Boolean.TRUE.equals(report.get("success")));
            assertEquals(42, ((Map<?, ?>) report.get("actual")).get("result"));
        } finally {
            Files.walk(tempDir)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                    }
                });
        }
    }

    private static Registry baseRegistry() {
        var registry = new Registry();
        FlowPrimitives.register(registry);
        CorePrimitives.register(registry);
        DemoPrimitives.register(registry);
        ToolingPrimitives.register(registry);
        return registry;
    }
}
