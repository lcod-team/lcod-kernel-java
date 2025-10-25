package work.lcod.kernel.tooling;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import work.lcod.kernel.runtime.ComposeLoader;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.KernelRegistry;

@CommandLine.Command(
    name = "spec-tests",
    description = "Run LCOD spec fixtures using the Java kernel",
    mixinStandardHelpOptions = true
)
public final class SpecTestRunner implements Callable<Integer> {
    private static final ObjectMapper JSON = new ObjectMapper();

    @CommandLine.Option(names = "--manifest", description = "Path to tests/conformance manifest (relative to spec repo)")
    private Path manifest;

    @CommandLine.Option(names = "--json", description = "Print JSON results (for conformance diffing)")
    private boolean json;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new SpecTestRunner()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        Path specRoot = locateSpecRepo();
        List<TestEntry> entries = manifest != null ? loadManifest(specRoot, manifest) : discoverAll(specRoot);
        if (entries.isEmpty()) {
            System.err.println("No spec fixtures found under " + specRoot);
            return 1;
        }

        List<TestResult> results = new ArrayList<>();
        for (TestEntry entry : entries) {
            results.add(runTest(entry));
        }

        long failures = results.stream().filter(r -> !r.success).count();
        if (json) {
            JSON.writeValue(System.out, results);
        } else {
            for (TestResult result : results) {
                if (result.success) {
                    System.out.println("✅ " + result.name);
                } else {
                    var suffix = result.error != null ? (" — " + result.error) : "";
                    System.err.println("❌ " + result.name + suffix);
                }
            }
        }
        return failures == 0 ? 0 : 1;
    }

    private TestResult runTest(TestEntry entry) {
        try {
            List<Map<String, Object>> compose = ComposeLoader.loadFromLocalFile(entry.composePath);
            var registry = KernelRegistry.create();
            var ctx = new ExecutionContext(registry, entry.composePath.getParent());
            Map<String, Object> state = ComposeRunner.runSteps(ctx, compose, Collections.emptyMap(), Map.of());
            Object reportObj = state.get("report");
            boolean success = reportObj instanceof Map<?, ?> reportMap
                ? Boolean.TRUE.equals(reportMap.get("success"))
                : true;
            return new TestResult(entry.name, success, reportObj, null);
        } catch (Exception ex) {
            return new TestResult(entry.name, false, null, ex.getMessage());
        }
    }

    private static Path locateSpecRepo() {
        String env = System.getenv("SPEC_REPO_PATH");
        if (env != null && !env.isBlank()) {
            Path candidate = Path.of(env).toAbsolutePath().normalize();
            if (Files.isDirectory(candidate.resolve("tests/spec"))) {
                return candidate;
            }
        }
        Path[] candidates = new Path[] {
            Path.of("../lcod-spec"),
            Path.of("../../lcod-spec"),
            Path.of("../spec/lcod-spec"),
            Path.of("../../spec/lcod-spec")
        };
        for (Path relative : candidates) {
            Path resolved = relative.toAbsolutePath().normalize();
            if (Files.isDirectory(resolved.resolve("tests/spec"))) {
                return resolved;
            }
        }
        throw new IllegalStateException("Unable to locate lcod-spec repository. Set SPEC_REPO_PATH.");
    }

    private static List<TestEntry> loadManifest(Path specRoot, Path manifestPath) throws IOException {
        Path resolved = manifestPath.isAbsolute() ? manifestPath : specRoot.resolve(manifestPath);
        List<Map<String, Object>> entries = JSON.readValue(Files.readString(resolved), new TypeReference<>() {});
        List<TestEntry> results = new ArrayList<>();
        for (Map<String, Object> entry : entries) {
            String name = String.valueOf(entry.get("name"));
            String compose = String.valueOf(entry.get("compose"));
            Path composePath = compose.startsWith("/") ? Path.of(compose) : specRoot.resolve(compose);
            results.add(new TestEntry(name, composePath.toAbsolutePath().normalize()));
        }
        return results;
    }

    private static List<TestEntry> discoverAll(Path specRoot) throws IOException {
        Path testsRoot = specRoot.resolve("tests/spec");
        List<TestEntry> entries = new ArrayList<>();
        try (var dirs = Files.list(testsRoot)) {
            dirs.filter(Files::isDirectory).forEach(dir -> {
                Path compose = dir.resolve("compose.yaml");
                if (Files.isRegularFile(compose)) {
                    entries.add(new TestEntry(dir.getFileName().toString(), compose));
                }
            });
        }
        return entries;
    }

    private record TestEntry(String name, Path composePath) {}

    private record TestResult(String name, boolean success, Object report, String error) {}
}
