package work.lcod.kernel.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

/**
 * Ensure the embedded runtime bundle is unpacked on disk so the kernel can operate without
 * external checkouts of lcod-spec/lcod-resolver.
 */
final class RuntimeBootstrap {
    private static final String RESOURCE_PATH = "/runtime/lcod-runtime.tar.gz";
    private static final String VERSION_MARKER = ".lcod-runtime-version";

    private RuntimeBootstrap() {}

    static synchronized Path ensureRuntime() {
        RuntimeHome runtime = resolveRuntimeHome();
        Path home = runtime.path();
        Path specRoot = findSpecRoot(home, runtime.version());

        System.setProperty("lcod.spec.root", specRoot.toString());
        System.setProperty("lcod.spec.runtime", specRoot.toString());
        System.setProperty("lcod.runtime.home", specRoot.toString());
        System.setProperty("lcod.resolver.root", specRoot.resolve("resolver").toString());
        System.setProperty("lcod.resolver.packages", specRoot.resolve("resolver").toString());
        System.setProperty("SPEC_REPO_PATH", specRoot.toString());
        System.setProperty("RESOLVER_REPO_PATH", specRoot.resolve("resolver").toString());

        if (!needsExtraction(runtime)) {
            return specRoot;
        }

        extractRuntimeBundle(runtime);
        return findSpecRoot(home, runtime.version());
    }

    private static RuntimeHome resolveRuntimeHome() {
        String envHome = System.getenv("LCOD_HOME");
        if (envHome != null && !envHome.isBlank()) {
            Path path = Path.of(envHome).toAbsolutePath().normalize();
            return new RuntimeHome(path, false, currentVersion());
        }
        Path managedPath = Path.of(
            System.getProperty("user.home"),
            ".lcod",
            "runtime",
            "java",
            currentVersion()
        ).toAbsolutePath().normalize();
        return new RuntimeHome(managedPath, true, currentVersion());
    }

    private static boolean needsExtraction(RuntimeHome runtime) {
        if (!runtime.managed()) {
            if (!Files.isDirectory(runtime.path())) {
                throw new IllegalStateException(
                    "LCOD_HOME points to " + runtime.path() + " but the directory does not exist"
                );
            }
            return false;
        }
        Path marker = runtime.path().resolve(VERSION_MARKER);
        if (!Files.exists(marker) || !Files.isRegularFile(marker)) {
            return true;
        }
        try {
            String recorded = Files.readString(marker).trim();
            return !Objects.equals(recorded, runtime.version());
        } catch (IOException ex) {
            return true;
        }
    }

    private static void extractRuntimeBundle(RuntimeHome runtime) {
        Path home = runtime.path();
        try {
            if (runtime.managed() && Files.exists(home)) {
                deleteRecursively(home);
            }
            Files.createDirectories(home);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to prepare runtime directory " + home + ": " + ex.getMessage(), ex);
        }

        try (InputStream raw = RuntimeBootstrap.class.getResourceAsStream(RESOURCE_PATH)) {
            if (raw == null) {
                throw new IllegalStateException("Embedded runtime bundle missing from resources (" + RESOURCE_PATH + ")");
            }
            try (
                GzipCompressorInputStream gzip = new GzipCompressorInputStream(raw);
                TarArchiveInputStream tar = new TarArchiveInputStream(gzip)
            ) {
                TarArchiveEntry entry;
                while ((entry = tar.getNextTarEntry()) != null) {
                    Path destination = home.resolve(entry.getName()).normalize();
                    if (!destination.startsWith(home)) {
                        throw new IllegalStateException("Refusing to extract entry outside runtime directory: " + entry.getName());
                    }
                    if (entry.isDirectory()) {
                        Files.createDirectories(destination);
                        continue;
                    }
                    if (entry.isSymbolicLink()) {
                        Path linkTarget = Path.of(entry.getLinkName());
                        createSymbolicLink(destination, linkTarget);
                        continue;
                    }
                    Files.createDirectories(destination.getParent());
                    Files.copy(tar, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    applyPermissions(destination, entry.getMode());
                }
            }
            Files.writeString(home.resolve(VERSION_MARKER), runtime.version());
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to extract embedded runtime: " + ex.getMessage(), ex);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void applyPermissions(Path file, int mode) {
        try {
            Set<PosixFilePermission> perms = modeToPermissions(mode);
            if (!perms.isEmpty()) {
                Files.setPosixFilePermissions(file, perms);
            }
        } catch (UnsupportedOperationException | IOException ignored) {
            // Non-POSIX filesystem or unable to set permissions; ignore.
        }
    }

    private static Set<PosixFilePermission> modeToPermissions(int mode) {
        EnumSet<PosixFilePermission> perms = EnumSet.noneOf(PosixFilePermission.class);
        if ((mode & 0400) != 0) perms.add(PosixFilePermission.OWNER_READ);
        if ((mode & 0200) != 0) perms.add(PosixFilePermission.OWNER_WRITE);
        if ((mode & 0100) != 0) perms.add(PosixFilePermission.OWNER_EXECUTE);
        if ((mode & 0040) != 0) perms.add(PosixFilePermission.GROUP_READ);
        if ((mode & 0020) != 0) perms.add(PosixFilePermission.GROUP_WRITE);
        if ((mode & 0010) != 0) perms.add(PosixFilePermission.GROUP_EXECUTE);
        if ((mode & 0004) != 0) perms.add(PosixFilePermission.OTHERS_READ);
        if ((mode & 0002) != 0) perms.add(PosixFilePermission.OTHERS_WRITE);
        if ((mode & 0001) != 0) perms.add(PosixFilePermission.OTHERS_EXECUTE);
        return perms;
    }

    private static void createSymbolicLink(Path link, Path target) throws IOException {
        try {
            if (Files.exists(link)) {
                Files.delete(link);
            }
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException ex) {
            // Platform does not support symlinks; best-effort fallback by copying file if possible.
            // No additional action required.
        }
    }

    private static Path findSpecRoot(Path home, String version) {
        Path candidate = home;
        if (hasSpecMarkers(candidate)) {
            return candidate;
        }
        String expectedDir = "lcod-runtime-v" + version;
        Path versioned = home.resolve(expectedDir);
        if (hasSpecMarkers(versioned)) {
            return versioned;
        }
        try {
            Path firstMatch = Files.list(home)
                .filter(path -> Files.isDirectory(path) && path.getFileName().toString().startsWith("lcod-runtime-"))
                .findFirst()
                .orElse(null);
            if (firstMatch != null && hasSpecMarkers(firstMatch)) {
                return firstMatch;
            }
        } catch (IOException ignored) {
        }
        return candidate;
    }

    private static boolean hasSpecMarkers(Path candidate) {
        if (candidate == null) {
            return false;
        }
        return Files.isDirectory(candidate.resolve("tooling"))
            && Files.isDirectory(candidate.resolve("tests/spec"));
    }

    private static String currentVersion() {
        return Optional.ofNullable(RuntimeBootstrap.class.getPackage().getImplementationVersion())
            .filter(v -> !v.isBlank())
            .orElse("development");
    }

    private record RuntimeHome(Path path, boolean managed, String version) {}
}
