package work.lcod.kernel.runtime;

import java.util.Map;

/**
 * Represents an executable function registered in the kernel registry.
 */
@FunctionalInterface
public interface KernelFunction {
    Object invoke(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception;
}
