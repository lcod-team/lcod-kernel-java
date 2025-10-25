package work.lcod.kernel.demo;

import java.util.Map;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;

/**
 * Demo/helper primitives mirroring the Node/Rust kernels so shared examples run identically.
 */
public final class DemoPrimitives {
    private DemoPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://impl/echo@1", DemoPrimitives::echo);
        registry.register("lcod://impl/is_even@1", DemoPrimitives::isEven);
        registry.register("lcod://impl/gt@1", DemoPrimitives::greaterThan);
        registry.register("lcod://impl/set@1", DemoPrimitives::setValues);
        return registry;
    }

    private static Object echo(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) {
        Object value = input.get("value");
        return Map.of("val", value, "value", value);
    }

    private static Object isEven(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) {
        long value = toLong(input.get("value"));
        boolean ok = (value % 2L) == 0L;
        return Map.of("ok", ok, "value", ok);
    }

    private static Object greaterThan(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) {
        long value = toLong(input.get("value"));
        long limit = toLong(input.get("limit"));
        boolean ok = value > limit;
        return Map.of("ok", ok, "value", ok);
    }

    private static Object setValues(ExecutionContext ctx, Map<String, Object> input, work.lcod.kernel.runtime.StepMeta meta) {
        return Map.copyOf(input);
    }

    private static long toLong(Object raw) {
        if (raw instanceof Number number) {
            return number.longValue();
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
