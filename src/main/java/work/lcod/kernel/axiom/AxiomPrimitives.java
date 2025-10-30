package work.lcod.kernel.axiom;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.tomlj.Toml;
import org.tomlj.TomlInvalidTypeException;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlArray;
import org.tomlj.TomlTable;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

public final class AxiomPrimitives {
    private AxiomPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://axiom/path/join@1", AxiomPrimitives::pathJoin);
        registry.register("lcod://axiom/fs/read-file@1", AxiomPrimitives::fsReadFile);
        registry.register("lcod://axiom/fs/list-dir@1", AxiomPrimitives::fsListDir);
        registry.register("lcod://axiom/toml/parse@1", AxiomPrimitives::tomlParse);
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

    private static Object fsReadFile(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String pathValue = input != null && input.get("path") != null ? String.valueOf(input.get("path")) : null;
        if (pathValue == null || pathValue.isBlank()) {
            throw new IllegalArgumentException("fs/read-file requires `path`");
        }
        String encoding = input != null && input.get("encoding") != null ? String.valueOf(input.get("encoding")) : "utf-8";
        Path resolved = resolve(ctx, pathValue);
        try {
            byte[] bytes = Files.readAllBytes(resolved);
            Map<String, Object> result = new LinkedHashMap<>();
            if ("utf-8".equalsIgnoreCase(encoding)) {
                result.put("data", new String(bytes, StandardCharsets.UTF_8));
            } else if ("base64".equalsIgnoreCase(encoding)) {
                result.put("data", java.util.Base64.getEncoder().encodeToString(bytes));
            } else {
                result.put("data", new String(bytes, StandardCharsets.UTF_8));
                result.put("warning", "unsupported encoding, returned utf-8");
            }
            result.put("encoding", encoding);
            result.put("size", bytes.length);
            FileTime mtime = Files.getLastModifiedTime(resolved);
            result.put("mtime", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(mtime.toInstant().atOffset(java.time.ZoneOffset.UTC)));
            return result;
        } catch (IOException ex) {
            throw new IllegalStateException("failed to read file: " + resolved + ": " + ex.getMessage(), ex);
        }
    }

    private static Object fsListDir(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String pathValue = input != null && input.get("path") != null ? String.valueOf(input.get("path")) : ".";
        Path root = resolve(ctx, pathValue);
        boolean recursive = input != null && Boolean.TRUE.equals(input.get("recursive"));
        List<Map<String, Object>> entries = new ArrayList<>();
        try {
            if (recursive) {
                Files.walk(root)
                    .skip(1)
                    .forEach(p -> entries.add(describePath(root, p)));
            } else {
                Files.list(root).forEach(p -> entries.add(describePath(root, p)));
            }
        } catch (IOException ex) {
            throw new IllegalStateException("failed to list directory: " + root + ": " + ex.getMessage(), ex);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("entries", entries);
        return result;
    }

    private static Map<String, Object> describePath(Path root, Path path) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("name", path.getFileName().toString());
        node.put("path", path.toString());
        if (Files.isDirectory(path)) {
            node.put("type", "directory");
        } else if (Files.isRegularFile(path)) {
            node.put("type", "file");
        } else {
            node.put("type", "other");
        }
        node.put("relativePath", root.relativize(path).toString());
        return node;
    }

    private static Object tomlParse(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String text = input != null && input.get("text") != null ? String.valueOf(input.get("text")) : "";
        TomlParseResult result = Toml.parse(text);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("errors", result.errors());
        if (result.hasErrors()) {
            payload.put("value", null);
        } else {
            try {
                payload.put("value", convertTomlMap(result.toMap()));
            } catch (TomlInvalidTypeException ex) {
                payload.put("value", Map.of());
                payload.put("errors", List.of(ex.getMessage()));
            }
        }
        return payload;
    }

    private static Map<String, Object> convertTomlMap(Map<String, Object> source) {
        Map<String, Object> converted = new LinkedHashMap<>();
        for (var entry : source.entrySet()) {
            converted.put(String.valueOf(entry.getKey()), convertTomlValue(entry.getValue()));
        }
        return converted;
    }

    private static Object convertTomlValue(Object value) {
        if (value instanceof TomlTable table) {
            return convertTomlMap(table.toMap());
        }
        if (value instanceof TomlArray array) {
            List<Object> items = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                items.add(convertTomlValue(array.get(i)));
            }
            return items;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), convertTomlValue(v)));
            return copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(convertTomlValue(item));
            }
            return copy;
        }
        return value;
    }

    private static Path resolve(ExecutionContext ctx, String value) {
        Path base = ctx.workingDirectory();
        Path provided = Path.of(value);
        return provided.isAbsolute() ? provided : base.resolve(provided);
    }
}
