package work.lcod.kernel.spec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class SpecPaths {
    private SpecPaths() {}

    public static Optional<Path> locateSpecRoot() {
        List<Path> baseCandidates = new ArrayList<>();
        addIfPresent(baseCandidates, envCandidate());
        addIfPresent(baseCandidates, propertyCandidate());
        baseCandidates.add(Path.of("../lcod-spec"));
        baseCandidates.add(Path.of("../../lcod-spec"));
        baseCandidates.add(Path.of("../spec/lcod-spec"));
        baseCandidates.add(Path.of("../../spec/lcod-spec"));
        Path runtimeHome = envRuntimeCandidate();
        if (runtimeHome != null) {
            baseCandidates.add(runtimeHome);
        }

        for (Path candidate : baseCandidates) {
            Path resolved = candidate.toAbsolutePath().normalize();
            if (Files.isDirectory(resolved) && Files.isDirectory(resolved.resolve("tests/spec"))) {
                return Optional.of(resolved);
            }
        }
        return Optional.empty();
    }

    private static Path envRuntimeCandidate() {
        String home = System.getenv("LCOD_HOME");
        if (home == null || home.isBlank()) {
            return null;
        }
        return Path.of(home);
    }

    private static void addIfPresent(List<Path> target, Path candidate) {
        if (candidate != null) {
            target.add(candidate);
        }
    }

    private static Path envCandidate() {
        String value = System.getenv("SPEC_REPO_PATH");
        if (value == null || value.isBlank()) {
            return null;
        }
        return Path.of(value);
    }

    private static Path propertyCandidate() {
        String value = System.getProperty("lcod.spec.root");
        if (value == null || value.isBlank()) {
            return null;
        }
        return Path.of(value);
    }
}
