package work.lcod.kernel.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class LcodRunnerTest {
    @Test
    void runsLocalComposeFile() {
        Path composePath = Path.of("src", "test", "resources", "composes", "echo.yaml").toAbsolutePath();
        LcodRunConfiguration config = LcodRunConfiguration.builder()
            .composeTarget(ComposeTarget.forLocal(composePath))
            .workingDirectory(composePath.getParent())
            .lockFile(composePath.getParent().resolve("lcp.lock"))
            .cacheDirectory(composePath.getParent().resolve(".lcod/cache"))
            .logLevel(LogLevel.INFO)
            .build();

        RunResult result = new LcodRunner().run(config);
        assertEquals(RunResult.Status.SUCCESS, result.status());
        assertTrue(result.metadata().containsKey("result"));
        Object echoed = ((java.util.Map<?, ?>) result.metadata().get("result")).get("echoed");
        assertEquals(123, echoed);
    }
}
