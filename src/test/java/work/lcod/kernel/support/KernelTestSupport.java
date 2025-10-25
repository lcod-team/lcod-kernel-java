package work.lcod.kernel.support;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.KernelRegistry;
import work.lcod.kernel.runtime.Registry;

/**
 * Shared helpers for kernel test suites. Mirrors the lightweight demo bindings
 * used in the Node/Rust tests so fixtures can exercise flow operators without
 * wiring the full resolver/tooling stack.
 */
public final class KernelTestSupport {
    private KernelTestSupport() {}

    public static ExecutionContext demoContext() {
        return new ExecutionContext(KernelRegistry.create());
    }

    public static Optional<Path> locateSpecRepo() {
        var env = System.getenv("SPEC_REPO_PATH");
        if (env != null && !env.isBlank()) {
            var candidate = Path.of(env).toAbsolutePath().normalize();
            if (isSpecRepo(candidate)) {
                return Optional.of(candidate);
            }
        }

        var candidates = List.of(
            Path.of("../lcod-spec"),
            Path.of("../../lcod-spec"),
            Path.of("../spec/lcod-spec"),
            Path.of("../../spec/lcod-spec")
        );

        for (var candidate : candidates) {
            var resolved = candidate.toAbsolutePath().normalize();
            if (isSpecRepo(resolved)) {
                return Optional.of(resolved);
            }
        }
        return Optional.empty();
    }

    private static boolean isSpecRepo(Path path) {
        return Files.isDirectory(path)
            && Files.isRegularFile(path.resolve("OVERVIEW.md"))
            && Files.isDirectory(path.resolve("tests/spec"));
    }

    private static void registerDemoImplementations(Registry registry) {
        registry.register("lcod://impl/set@1", (ctx, input, meta) -> new LinkedHashMap<>(input));
        registry.register("lcod://impl/echo@1", (ctx, input, meta) -> {
            Object value = input.get("value");
            return Map.ofEntries(
                Map.entry("value", value),
                Map.entry("val", value)
            );
        });
        registry.register("lcod://impl/is_even@1", (ctx, input, meta) -> {
            boolean ok = asLong(input.get("value")) % 2 == 0;
            return boolResult(ok);
        });
        registry.register("lcod://impl/gt@1", (ctx, input, meta) -> {
            long value = asLong(input.get("value"));
            long limit = asLong(input.get("limit"));
            return boolResult(value > limit);
        });
    }

    private static Map<String, Object> boolResult(boolean value) {
        return Map.ofEntries(
            Map.entry("ok", value),
            Map.entry("value", value)
        );
    }

    private static long asLong(Object raw) {
        if (raw instanceof Number num) {
            return num.longValue();
        }
        if (raw instanceof String str && !str.isBlank()) {
            try {
                return Long.parseLong(str.trim());
            } catch (NumberFormatException ignored) {
                return 0L;
            }
        }
        return 0L;
    }
}
