package work.lcod.kernel.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;

class CoreEnvPrimitivesTest {
    @Test
    void envGetReturnsValues() throws Exception {
        var registry = registry();
        var ctx = new ExecutionContext(registry);
        String existingKey = System.getenv().keySet().stream().findFirst().orElse("PATH");
        @SuppressWarnings("unchecked")
        var hit = (Map<String, Object>) ctx.call("lcod://contract/core/env/get@1", Map.of("name", existingKey), null);
        assertEquals(true, hit.get("exists"));

        @SuppressWarnings("unchecked")
        var miss = (Map<String, Object>) ctx.call(
            "lcod://contract/core/env/get@1",
            Map.of("name", "LCOD_TEST_ENV_JAVA_MISSING", "default", "fallback"),
            null
        );
        assertEquals("fallback", miss.get("value"));
    }

    @Test
    void runtimeInfoExposesDirectories() throws Exception {
        var registry = registry();
        var ctx = new ExecutionContext(registry);
        @SuppressWarnings("unchecked")
        var info = (Map<String, Object>) ctx.call("lcod://contract/core/runtime/info@1", Map.of(), null);
        assertNotNull(info.get("cwd"));
        assertNotNull(info.get("tmpDir"));
    }

    @Test
    void fsStatReportsExistence() throws Exception {
        var registry = registry();
        CoreFsPrimitives.register(registry);
        var ctx = new ExecutionContext(registry);
        Path tmp = Files.createTempFile("lcod-java-stat", ".txt");
        Files.writeString(tmp, "demo");
        @SuppressWarnings("unchecked")
        var stat = (Map<String, Object>) ctx.call(
            "lcod://contract/core/fs/stat@1",
            Map.of("path", tmp.toString()),
            null
        );
        assertTrue((Boolean) stat.get("exists"));
        @SuppressWarnings("unchecked")
        var missing = (Map<String, Object>) ctx.call(
            "lcod://contract/core/fs/stat@1",
            Map.of("path", tmp.getParent().resolve("missing.txt").toString()),
            null
        );
        assertFalse((Boolean) missing.get("exists"));
    }

    private Registry registry() {
        var registry = new Registry();
        CoreEnvPrimitives.register(registry);
        return registry;
    }
}
