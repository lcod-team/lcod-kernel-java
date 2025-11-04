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
import work.lcod.kernel.runtime.KernelRegistry;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

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
        if (!Boolean.TRUE.equals(report.get("success"))) {
            org.junit.jupiter.api.Assertions.fail("report=" + report);
        }
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

    @Test
    void testCheckerInjectsStreams() throws Exception {
        var ctx = new ExecutionContext(KernelRegistry.create());
        var compose = List.of(Map.<String, Object>of(
            "call", "lcod://tooling/test_checker@1",
            "in", Map.of(
                "compose", List.of(
                    Map.of(
                        "call", "lcod://contract/core/stream/read@1",
                        "in", Map.of(
                            "stream", "$.numbers.stream",
                            "decode", "utf-8",
                            "maxBytes", 2
                        ),
                        "out", Map.of("read", "$")
                    ),
                    Map.of(
                        "call", "lcod://contract/core/stream/close@1",
                        "in", Map.of("stream", "$.numbers.stream")
                    ),
                    Map.of(
                        "call", "lcod://tooling/script@1",
                        "in", Map.of(
                            "source", "({ input }) => ({ success: input.chunk === '12', chunk: input.chunk })",
                            "bindings", Map.of(
                                "chunk", Map.of("path", "$.read.chunk")
                            )
                        ),
                        "out", Map.of("report", "$")
                    )
                ),
                "input", Map.of("numbers", Map.of()),
                "streams", List.of(Map.of(
                    "target", "numbers.stream",
                    "encoding", "utf-8",
                    "chunks", List.of("12")
                )),
                "expected", Map.of(
                    "report", Map.of()
                )
            ),
            "out", Map.of("report", "$")
        ));

        var state = ComposeRunner.runSteps(ctx, compose, new LinkedHashMap<>(), Map.of());
        var report = (Map<?, ?>) state.get("report");
        var actual = (Map<?, ?>) report.get("actual");
        var read = (Map<?, ?>) actual.get("read");
        assertEquals("12", read.get("chunk"));
    }

    @Test
    void jsonlReadParsesEntries() throws Exception {
        Path manifest = Files.createTempFile("lcod-jsonl", ".jsonl");
        try {
            Files.writeString(manifest, "{\"type\":\"manifest\",\"schema\":\"lcod-manifest/list@1\"}\n" +
                "{\"type\":\"component\",\"id\":\"lcod://example/foo@0.1.0\"}\n" +
                "{\"type\":\"list\",\"path\":\"nested.jsonl\"}\n");

            var registry = baseRegistry();
            var ctx = new ExecutionContext(registry);
            @SuppressWarnings("unchecked")
            var result = (Map<String, Object>) ctx.call(
                "lcod://tooling/jsonl/read@0.1.0",
                Map.of("path", manifest.toString()),
                new StepMeta(Map.of(), Map.of(), null)
            );

            @SuppressWarnings("unchecked")
            var entries = (List<Map<String, Object>>) result.get("entries");
            assertEquals(3, entries.size());
            assertEquals("component", entries.get(1).get("type"));
            assertEquals("nested.jsonl", entries.get(2).get("path"));
            assertTrue(((List<?>) result.get("warnings")).isEmpty());
        } finally {
            Files.deleteIfExists(manifest);
        }
    }

    @Test
    void jsonlReadCollectsWarnings() throws Exception {
        Path manifest = Files.createTempFile("lcod-jsonl", ".jsonl");
        try {
            Files.writeString(manifest, "{\"type\":\"manifest\",\"schema\":\"lcod-manifest/list@1\"}\n" +
                "not json\n" +
                "{\"type\":\"component\",\"id\":\"lcod://example/foo@0.1.0\"}\n");

            var registry = baseRegistry();
            var ctx = new ExecutionContext(registry);
            @SuppressWarnings("unchecked")
            var result = (Map<String, Object>) ctx.call(
                "lcod://tooling/jsonl/read@0.1.0",
                Map.of("path", manifest.toString()),
                new StepMeta(Map.of(), Map.of(), null)
            );

            @SuppressWarnings("unchecked")
            var entries = (List<Map<String, Object>>) result.get("entries");
            assertEquals(2, entries.size());
            @SuppressWarnings("unchecked")
            var warnings = (List<String>) result.get("warnings");
            assertEquals(1, warnings.size());
            assertTrue(warnings.get(0).contains("invalid JSONL entry"));
        } finally {
            Files.deleteIfExists(manifest);
        }
    }
}
