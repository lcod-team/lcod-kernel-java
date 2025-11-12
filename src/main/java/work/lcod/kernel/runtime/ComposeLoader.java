package work.lcod.kernel.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

/**
 * Loads compose definitions (local path or HTTP URL) into in-memory step maps.
 */
public final class ComposeLoader {
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private ComposeLoader() {}

    public static List<Map<String, Object>> loadFromLocalFile(Path path) {
        try (var in = Files.newInputStream(path)) {
            var steps = parseCompose(in);
            var context = resolveContext(path);
            if (context != null) {
                canonicalizeSteps(steps, context);
            }
            return steps;
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to read compose: " + path, ex);
        }
    }

    public static List<Map<String, Object>> loadFromHttp(URI uri) {
        try {
            var client = HttpClient.newHttpClient();
            var request = HttpRequest.newBuilder(uri).GET().build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() >= 400) {
                throw new IllegalStateException("HTTP " + response.statusCode() + " while downloading compose: " + uri);
            }
            try (var body = response.body()) {
                return parseCompose(body);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while downloading compose: " + uri, ex);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to download compose: " + uri, ex);
        }
    }

    private static List<Map<String, Object>> parseCompose(InputStream in) throws IOException {
        var root = YAML_MAPPER.readTree(in);
        if (root == null || !root.hasNonNull("compose")) {
            return List.of();
        }
        var composeNode = root.get("compose");
        if (!composeNode.isArray()) {
            return List.of();
        }
        var steps = new ArrayList<Map<String, Object>>();
        for (var stepNode : composeNode) {
            steps.add(toMap(stepNode));
        }
        return steps;
    }

    private static Map<String, Object> toMap(JsonNode node) throws IOException {
        if (node == null || node.isNull()) {
            return Map.of();
        }
        if (!node.isObject()) {
            throw new IOException("Compose step must be an object: " + node);
        }
        var map = new LinkedHashMap<String, Object>();
        var fields = node.fields();
        while (fields.hasNext()) {
            var entry = fields.next();
            map.put(entry.getKey(), convertNode(entry.getValue()));
        }
        return map;
    }

    private static Object convertNode(JsonNode node) throws IOException {
        if (node.isObject()) {
            var map = new LinkedHashMap<String, Object>();
            var fields = node.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                map.put(entry.getKey(), convertNode(entry.getValue()));
            }
            return map;
        }
        if (node.isArray()) {
            var list = new ArrayList<Object>();
            for (var item : node) {
                list.add(convertNode(item));
            }
            return list;
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isNull()) {
            return null;
        }
        return node.asText();
    }

    private static void canonicalizeSteps(List<Map<String, Object>> steps, ComposeContext context) {
        for (var step : steps) {
            canonicalizeValue(step, context);
        }
    }

    @SuppressWarnings("unchecked")
    private static void canonicalizeValue(Object value, ComposeContext context) {
        if (value instanceof Map<?, ?> map) {
            canonicalizeObject((Map<String, Object>) map, context);
        } else if (value instanceof List<?> list) {
            for (var item : list) {
                canonicalizeValue(item, context);
            }
        }
    }

    private static void canonicalizeObject(Map<String, Object> map, ComposeContext context) {
        Object callValue = map.get("call");
        if (callValue instanceof String raw) {
            map.put("call", canonicalizeId(raw, context));
        }
        for (var entry : map.entrySet()) {
            if ("call".equals(entry.getKey())) {
                continue;
            }
            canonicalizeValue(entry.getValue(), context);
        }
    }

    private static String canonicalizeId(String raw, ComposeContext context) {
        if (raw == null || raw.isBlank() || context == null) {
            return raw;
        }
        String trimmed = raw.trim();
        if (trimmed.startsWith("lcod://")) {
            return trimmed;
        }
        String normalized = trimmed.startsWith("./") ? trimmed.substring(2) : trimmed;
        String[] segments = normalized.split("/");
        if (segments.length == 0) {
            return trimmed;
        }
        String first = segments[0];
        String mapped = context.aliases().getOrDefault(first, first);

        List<String> parts = new ArrayList<>();
        if (!context.basePath().isBlank()) {
            parts.add(context.basePath());
        }
        if (!mapped.isBlank()) {
            parts.add(mapped);
        }
        for (int i = 1; i < segments.length; i++) {
            if (!segments[i].isBlank()) {
                parts.add(segments[i]);
            }
        }
        String base = String.join("/", parts);
        if (base.isBlank()) {
            base = normalized.replace('.', '/');
        }
        if (base.isBlank()) {
            return trimmed;
        }
        String version = context.version().isBlank() ? "0.0.0" : context.version();
        return "lcod://" + base + "@" + version;
    }

    private static ComposeContext resolveContext(Path composePath) {
        Map<String, String> aliases = new LinkedHashMap<>();
        String basePath = "";
        String version = "";

        Path manifestPath = composePath.resolveSibling("lcp.toml");
        TomlParseResult manifest = parseToml(manifestPath);
        if (manifest != null) {
            basePath = deriveBasePath(manifest);
            version = Optional.ofNullable(manifest.getString("version")).orElse("");
            TomlTable workspaceSection = manifest.getTable("workspace");
            if (workspaceSection != null) {
                TomlTable scopeAliases = workspaceSection.getTable("scopeAliases");
                if (scopeAliases != null) {
                    aliases.putAll(readAliasEntries(scopeAliases));
                }
            }
        }

        TomlParseResult workspaceManifest = parseToml(findWorkspaceManifest(composePath));
        if (workspaceManifest != null) {
            TomlTable workspace = workspaceManifest.getTable("workspace");
            if (workspace != null) {
                Map<String, String> workspaceAliases = readAliasEntries(workspace.getTable("scopeAliases"));
                workspaceAliases.putAll(aliases);
                aliases = workspaceAliases;
            }
            if (basePath.isBlank()) {
                basePath = deriveBasePath(workspaceManifest);
            }
            if (version.isBlank()) {
                version = Optional.ofNullable(workspaceManifest.getString("version")).orElse("");
            }
        }

        if (basePath.isBlank() && aliases.isEmpty()) {
            return null;
        }
        return new ComposeContext(basePath, version, aliases);
    }

    private static TomlParseResult parseToml(Path path) {
        if (path == null || !Files.isRegularFile(path)) {
            return null;
        }
        try {
            TomlParseResult result = Toml.parse(Files.readString(path));
            if (result.hasErrors()) {
                return null;
            }
            return result;
        } catch (IOException ex) {
            return null;
        }
    }

    private static Path findWorkspaceManifest(Path composePath) {
        Path current = composePath.toAbsolutePath().getParent();
        while (current != null) {
            Path candidate = current.resolve("workspace.lcp.toml");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        return null;
    }

    private static Map<String, String> readAliasEntries(TomlTable table) {
        Map<String, String> aliases = new LinkedHashMap<>();
        if (table == null || table.isEmpty()) {
            return aliases;
        }
        for (String key : table.keySet()) {
            String value = Optional.ofNullable(table.getString(key)).orElse("");
            if (!value.isBlank()) {
                aliases.put(key, value);
            }
        }
        return aliases;
    }

    private static String deriveBasePath(TomlParseResult manifest) {
        if (manifest == null) {
            return "";
        }
        String id = manifest.getString("id");
        if (id != null && id.startsWith("lcod://")) {
            String trimmed = id.substring("lcod://".length());
            int at = trimmed.indexOf('@');
            return at >= 0 ? trimmed.substring(0, at) : trimmed;
        }
        String namespace = Optional.ofNullable(manifest.getString("namespace")).orElse("");
        String name = Optional.ofNullable(manifest.getString("name")).orElse("");
        if (!namespace.isEmpty() && !name.isEmpty()) {
            return namespace + "/" + name;
        }
        if (!namespace.isEmpty()) {
            return namespace;
        }
        return name;
    }

    private record ComposeContext(String basePath, String version, Map<String, String> aliases) {}
}
