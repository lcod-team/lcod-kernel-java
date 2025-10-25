package work.lcod.kernel.tooling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.KernelRegistry;

final class ScriptFixturesTest {
    @Test
    void scriptRangeFixture() throws Exception {
        var ctx = new ExecutionContext(KernelRegistry.create());
        var compose = List.of(Map.of(
            "call", "lcod://tooling/test_checker@1",
            "in", Map.of(
                "compose", List.of(Map.of(
                    "call", "lcod://tooling/script@1",
                    "in", Map.of(
                        "source", "({ input }) => ({ success: input.value > 0, result: input.value })",
                        "input", Map.of("value", 5),
                        "bindings", Map.of(
                            "value", Map.of("path", "$.value")
                        )
                    ),
                    "out", Map.of("report", "$")
                )),
                "expected", Map.of(
                    "report", Map.of(
                        "success", true,
                        "result", 5
                    )
                )
            ),
            "out", Map.of("report", "$")
        ));

        Map<String, Object> state = ComposeRunner.runSteps(ctx, compose, new LinkedHashMap<>(), Map.of());
        Map<?, ?> report = (Map<?, ?>) state.get("report");
        assertTrue(Boolean.TRUE.equals(report.get("success")), () -> "scriptTools report=" + report);
        Map<?, ?> actual = (Map<?, ?>) report.get("actual");
        Map<?, ?> scriptReport = (Map<?, ?>) actual.get("report");
        assertTrue(Boolean.TRUE.equals(scriptReport.get("success")));
        assertEquals(5, scriptReport.get("result"));
    }

    @Test
    void scriptRunSlotFixture() throws Exception {
        var ctx = new ExecutionContext(KernelRegistry.create());
        var compose = List.of(Map.of(
            "call", "lcod://tooling/test_checker@1",
            "in", Map.of(
                "compose", List.of(Map.of(
                    "call", "lcod://tooling/script@1",
                    "in", Map.of(
                        "source", "async ({ state }, api) => { api.log(`script:${state.value}`); const child = await api.runSlot('child', { payload: state.value }); return { success: true, child }; }",
                        "input", Map.of("value", 4)
                    ),
                    "slots", Map.of(
                        "child", List.of(Map.of(
                            "call", "lcod://impl/echo@1",
                            "in", Map.of("value", "$.payload"),
                            "out", Map.of("result", "$")
                        ))
                    ),
                    "out", Map.of("report", "$")
                )),
                "expected", Map.of(
                    "report", Map.of(
                        "success", true,
                        "child", Map.of(
                            "result", Map.of("val", 4)
                        ),
                        "messages", List.of("script:4")
                    )
                )
            ),
            "out", Map.of("report", "$")
        ));

        Map<String, Object> state = ComposeRunner.runSteps(ctx, compose, new LinkedHashMap<>(), Map.of());
        Map<?, ?> report = (Map<?, ?>) state.get("report");
        assertTrue(Boolean.TRUE.equals(report.get("success")));
        Map<?, ?> actual = (Map<?, ?>) report.get("actual");
        Map<?, ?> scriptReport = (Map<?, ?>) actual.get("report");
        assertTrue(Boolean.TRUE.equals(scriptReport.get("success")));
        assertEquals("script:4", ((List<?>) scriptReport.get("messages")).get(0));
        Map<?, ?> child = (Map<?, ?>) scriptReport.get("child");
        assertEquals(4, ((Map<?, ?>) child.get("result")).get("val"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void scriptToolsFixture() throws Exception {
        var ctx = new ExecutionContext(KernelRegistry.create());
        var compose = List.of(Map.of(
            "call", "lcod://tooling/test_checker@1",
            "in", Map.of(
                "compose", List.of(Map.of(
                    "call", "lcod://tooling/script@1",
                    "in", Map.of(
                        "source", """
                            async ({ input, imports }, api) => {
                              const hash = await imports.hash({ data: String(input.value), encoding: 'utf-8' });
                              const doubled = await api.run('double', { value: input.value });
                              const guarded = await api.run('guard', doubled);
                              const config = api.config();
                              if (!guarded.success) {
                                return { success: false, messages: [guarded.message ?? 'guard failed'] };
                              }
                              return {
                                success: config.feature.enabled === true,
                                report: {
                                  doubled: guarded.value,
                                  config,
                                  hashHex: hash.hex
                                }
                              };
                            }
                            """,
                        "bindings", Map.of("value", Map.of("value", 6)),
                        "imports", Map.of("hash", "lcod://contract/core/hash/sha256@1"),
                        "config", Map.of(
                            "feature", Map.of("enabled", true),
                            "thresholds", Map.of("double", 2)
                        ),
                        "tools", List.of(
                            Map.of(
                                "name", "double",
                                "source", """
                                    ({ value }, api) => {
                                      api.log('tool.double', value);
                                      return { success: true, value: value * 2 };
                                    }
                                    """
                            ),
                            Map.of(
                                "name", "guard",
                                "source", """
                                    ({ value }, api) => {
                                      const min = api.config('thresholds.double', 1);
                                      if (value < min) {
                                        return { success: false, message: `value ${value} below min ${min}` };
                                      }
                                      api.log('guard.ok', value);
                                      return { success: true, value };
                                    }
                                    """
                            )
                        ),
                        "timeoutMs", 500
                    ),
                    "out", Map.of("report", "$")
                )),
                "expected", Map.of(
                    "report", Map.of(
                        "success", true,
                        "report", Map.of(
                            "doubled", 12,
                            "config", Map.of(
                                "feature", Map.of("enabled", true),
                                "thresholds", Map.of("double", 2)
                            ),
                            "hashHex", "e7f6c011776e8db7cd330b54174fd76f7d0216b612387a5ffcfb81e6f0919683"
                        )
                    )
                )
            ),
            "out", Map.of("report", "$")
        ));

        Map<String, Object> state = ComposeRunner.runSteps(ctx, compose, new LinkedHashMap<>(), Map.of());
        Map<?, ?> report = (Map<?, ?>) state.get("report");
        assertTrue(Boolean.TRUE.equals(report.get("success")));
        Map<?, ?> actual = (Map<?, ?>) report.get("actual");
        Map<?, ?> scriptReport = (Map<?, ?>) actual.get("report");
        assertTrue(Boolean.TRUE.equals(scriptReport.get("success")));
        Map<?, ?> inner = (Map<?, ?>) scriptReport.get("report");
        assertEquals(12, inner.get("doubled"));
        assertEquals("e7f6c011776e8db7cd330b54174fd76f7d0216b612387a5ffcfb81e6f0919683", inner.get("hashHex"));
        Map<?, ?> config = (Map<?, ?>) inner.get("config");
        Map<?, ?> feature = (Map<?, ?>) config.get("feature");
        assertEquals(Boolean.TRUE, feature.get("enabled"));
    }

}
