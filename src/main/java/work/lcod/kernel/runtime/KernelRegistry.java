package work.lcod.kernel.runtime;

import java.util.Map;
import work.lcod.kernel.core.CorePrimitives;
import work.lcod.kernel.demo.DemoPrimitives;
import work.lcod.kernel.flow.FlowPrimitives;
import work.lcod.kernel.tooling.ToolingPrimitives;

/**
 * Shared registry bootstrap so CLI, spec runner, and tests use the same helper set.
 */
public final class KernelRegistry {
    private KernelRegistry() {}

    public static Registry create() {
        var registry = new Registry();
        registry.register("lcod://kernel/log@1", (ctx, input, meta) -> Map.of());
        FlowPrimitives.register(registry);
        CorePrimitives.register(registry);
        DemoPrimitives.register(registry);
        ToolingPrimitives.register(registry);
        return registry;
    }
}
