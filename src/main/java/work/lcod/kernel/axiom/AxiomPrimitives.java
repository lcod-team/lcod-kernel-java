package work.lcod.kernel.axiom;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

public final class AxiomPrimitives {
    private AxiomPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://axiom/path/join@1", AxiomPrimitives::pathJoin);
        return registry;
    }

    private static Object pathJoin(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String base = input != null && input.get("base") != null ? String.valueOf(input.get("base")) : ".";
        Object segment = input != null ? input.get("segment") : null;
        if (segment == null && input != null && input.get("segments") instanceof Iterable<?> iterable) {
            Path accumulator = resolve(ctx, base);
            for (Object part : iterable) {
                accumulator = accumulator.resolve(String.valueOf(part));
            }
            return Map.of("path", accumulator.normalize().toString());
        }
        Path resolved = resolve(ctx, base).resolve(String.valueOf(segment == null ? "" : segment)).normalize();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("path", resolved.toString());
        result.put("exists", java.nio.file.Files.exists(resolved));
        return result;
    }

    private static Path resolve(ExecutionContext ctx, String value) {
        Path base = ctx.workingDirectory();
        Path provided = Path.of(value);
        return provided.isAbsolute() ? provided : base.resolve(provided);
    }
}
