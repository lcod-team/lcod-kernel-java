package work.lcod.kernel.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

/**
 * Filesystem helpers matching the contract semantics required by the spec fixtures.
 */
public final class CoreFsPrimitives {
    private CoreFsPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://core/fs/write-file@1", CoreFsPrimitives::writeFile);
        registry.register("lcod://contract/core/fs/write-file@1", CoreFsPrimitives::writeFile);
        registry.register("lcod://contract/core/fs/write_file@1", CoreFsPrimitives::writeFile);
        registry.register("lcod://axiom/fs/write-file@1", CoreFsPrimitives::writeFile);

        registry.register("lcod://core/fs/read-file@1", CoreFsPrimitives::readFile);
        registry.register("lcod://contract/core/fs/read-file@1", CoreFsPrimitives::readFile);
        registry.register("lcod://contract/core/fs/read_file@1", CoreFsPrimitives::readFile);

        registry.register("lcod://core/fs/list-dir@1", CoreFsPrimitives::listDir);
        registry.register("lcod://contract/core/fs/list-dir@1", CoreFsPrimitives::listDir);
        registry.register("lcod://contract/core/fs/list_dir@1", CoreFsPrimitives::listDir);
        registry.register("lcod://contract/core/fs/stat@1", CoreFsPrimitives::statPath);

        return registry;
    }

    private static Object writeFile(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws IOException {
        Path target = resolvePath(ctx, input.get("path"));
        if (target == null) {
            throw new IllegalArgumentException("path is required");
        }
        boolean createParents = Boolean.TRUE.equals(input.get("createParents"));
        if (createParents) {
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        }
        byte[] data = decodeData(String.valueOf(input.getOrDefault("data", "")), OptionalString.of(input.get("encoding")));
        Files.write(target, data, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
        Instant mtime = Files.getLastModifiedTime(target).toInstant();
        return Map.of(
            "bytesWritten", data.length,
            "mtime", mtime.toString(),
            "path", target.toString()
        );
    }

    private static Object readFile(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws IOException {
        Path target = resolvePath(ctx, input.get("path"));
        if (target == null) {
            throw new IllegalArgumentException("path is required");
        }
        byte[] data = Files.readAllBytes(target);
        String encoding = OptionalString.of(input.get("encoding")).orElse("utf-8").toLowerCase(Locale.ROOT);
        String encoded = encodeData(data, encoding);
        var attrs = Files.readAttributes(target, BasicFileAttributes.class);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("data", encoded);
        result.put("encoding", encoding);
        result.put("size", data.length);
        result.put("mtime", attrs.lastModifiedTime().toInstant().toString());
        return result;
    }

    private static Object listDir(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws IOException {
        Path dir = resolvePath(ctx, input.get("path"));
        if (dir == null) {
            throw new IllegalArgumentException("path is required");
        }
        boolean includeHidden = Boolean.TRUE.equals(input.get("includeHidden"));
        boolean includeStats = Boolean.TRUE.equals(input.get("includeStats"));
        boolean recursive = Boolean.TRUE.equals(input.get("recursive"));
        int maxDepth = input.get("maxDepth") instanceof Number n ? Math.max(1, n.intValue()) : (recursive ? Integer.MAX_VALUE : 1);

        List<Map<String, Object>> entries = new ArrayList<>();
        if (recursive) {
            Files.walkFileTree(dir, EnumSet.noneOf(FileVisitOption.class), maxDepth, new ListingVisitor(dir, includeHidden, includeStats, entries));
        } else {
            try (var stream = Files.list(dir)) {
                for (Path path : stream.collect(Collectors.toList())) {
                    if (!includeHidden && path.getFileName().toString().startsWith(".")) {
                        continue;
                    }
                    entries.add(describeEntry(dir, path, includeStats));
                }
            }
        }
        entries.sort((a, b) -> String.valueOf(a.get("name")).compareToIgnoreCase(String.valueOf(b.get("name"))));
        return Map.of("entries", entries);
    }

    private static Object statPath(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws IOException {
        Path target = resolvePath(ctx, input.get("path"));
        if (target == null) {
            throw new IllegalArgumentException("path is required");
        }
        boolean follow = !Boolean.FALSE.equals(input.get("followSymlinks"));
        BasicFileAttributes attrs;
        try {
            if (follow) {
                attrs = Files.readAttributes(target, BasicFileAttributes.class);
            } else {
                attrs = Files.readAttributes(target, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            }
        } catch (IOException ex) {
            if (Files.notExists(target)) {
                return Map.of(
                    "path", target.toAbsolutePath().normalize().toString(),
                    "exists", false
                );
            }
            throw ex;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("path", target.toAbsolutePath().normalize().toString());
        result.put("exists", true);
        result.put("isFile", attrs.isRegularFile());
        result.put("isDirectory", attrs.isDirectory());
        result.put("isSymlink", Files.isSymbolicLink(target));
        result.put("size", attrs.size());
        result.put("mtime", attrs.lastModifiedTime().toInstant().toString());
        result.put("ctime", attrs.creationTime().toInstant().toString());
        return result;
    }

    private static Map<String, Object> describeEntry(Path root, Path entry, boolean includeStats) throws IOException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("name", entry.getFileName().toString());
        result.put("path", entry.toAbsolutePath().normalize().toString());
        result.put("relativePath", root.relativize(entry).toString());
        String type = Files.isDirectory(entry)
            ? "directory"
            : Files.isSymbolicLink(entry) ? "symlink" : "file";
        result.put("type", type);
        if (includeStats) {
            var attrs = Files.readAttributes(entry, BasicFileAttributes.class);
            result.put("size", attrs.size());
            result.put("mtime", attrs.lastModifiedTime().toInstant().toString());
        }
        return result;
    }

    private static Path resolvePath(ExecutionContext ctx, Object raw) {
        if (raw == null) {
            return null;
        }
        Path base = ctx.workingDirectory();
        Path provided = Path.of(String.valueOf(raw));
        return provided.isAbsolute() ? provided.normalize() : base.resolve(provided).normalize();
    }

    private static byte[] decodeData(String value, OptionalString encodingOpt) {
        String encoding = encodingOpt.orElse("utf-8").toLowerCase(Locale.ROOT);
        return switch (encoding) {
            case "utf-8", "utf8" -> value.getBytes(StandardCharsets.UTF_8);
            case "base64" -> java.util.Base64.getDecoder().decode(value);
            case "hex" -> decodeHex(value);
            default -> value.getBytes(StandardCharsets.UTF_8);
        };
    }

    private static String encodeData(byte[] data, String encoding) {
        return switch (encoding) {
            case "utf-8", "utf8" -> new String(data, StandardCharsets.UTF_8);
            case "base64" -> java.util.Base64.getEncoder().encodeToString(data);
            case "hex" -> toHex(data);
            default -> new String(data, StandardCharsets.UTF_8);
        };
    }

    private static byte[] decodeHex(String input) {
        int len = input.length();
        byte[] out = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            out[i / 2] = (byte) ((Character.digit(input.charAt(i), 16) << 4) + Character.digit(input.charAt(i + 1), 16));
        }
        return out;
    }

    private static String toHex(byte[] data) {
        StringBuilder builder = new StringBuilder(data.length * 2);
        for (byte b : data) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    private record OptionalString(String value) {
        static OptionalString of(Object raw) {
            if (raw == null) return new OptionalString(null);
            String str = String.valueOf(raw).trim();
            return new OptionalString(str.isEmpty() ? null : str);
        }

        String orElse(String fallback) {
            return value == null ? fallback : value;
        }
    }

    private static final class ListingVisitor extends SimpleFileVisitor<Path> {
        private final Path root;
        private final boolean includeHidden;
        private final boolean includeStats;
        private final List<Map<String, Object>> entries;

        ListingVisitor(Path root, boolean includeHidden, boolean includeStats, List<Map<String, Object>> entries) {
            this.root = root;
            this.includeHidden = includeHidden;
            this.includeStats = includeStats;
            this.entries = entries;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (!includeHidden && file.getFileName().toString().startsWith(".")) {
                return FileVisitResult.CONTINUE;
            }
            entries.add(CoreFsPrimitives.describeEntry(root, file, includeStats));
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if (!Objects.equals(dir, root)) {
                if (!includeHidden && dir.getFileName().toString().startsWith(".")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                entries.add(CoreFsPrimitives.describeEntry(root, dir, includeStats));
            }
            return FileVisitResult.CONTINUE;
        }
    }
}
