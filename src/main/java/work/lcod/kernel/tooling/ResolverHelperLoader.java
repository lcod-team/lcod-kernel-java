package work.lcod.kernel.tooling;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.tomlj.Toml;
import org.tomlj.TomlArray;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;
import work.lcod.kernel.runtime.ComponentMetadata;
import work.lcod.kernel.runtime.ComponentMetadataLoader;
import work.lcod.kernel.runtime.ComposeLoader;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

/**
 * Loads resolver helper components from local workspaces or additional component paths.
 * This mirrors the behaviour implemented in the Rust/Node kernels so that developers can
 * execute composes directly from a checkout without publishing to a registry first.
 */
final class ResolverHelperLoader {
    private static final Map<Path, List<Map<String, Object>>> COMPOSE_CACHE = new ConcurrentHashMap<>();
    private static final Pattern PATH_SEPARATOR = Pattern.compile(Pattern.quote(String.valueOf(java.io.File.pathSeparatorChar)));

    private ResolverHelperLoader() {}

    static void registerWorkspaceHelpers(Registry registry) {
        List<HelperDefinition> definitions = collectDefinitions();
        if (definitions.isEmpty()) {
            return;
        }
        Map<String, HelperDefinition> deduplicated = new LinkedHashMap<>();
        for (HelperDefinition def : definitions) {
            deduplicated.put(def.id(), def);
        }
        for (HelperDefinition def : deduplicated.values()) {
            registerDefinition(registry, def);
            for (String alias : def.aliases()) {
                registerAlias(registry, alias, def);
            }
        }
    }

    private static void registerDefinition(Registry registry, HelperDefinition def) {
        registry.register(def.id(), (ctx, input, meta) -> invokeHelper(def, ctx, input), def.outputs(), def.metadata());
    }

    private static void registerAlias(Registry registry, String alias, HelperDefinition def) {
        if (alias == null || alias.isBlank()) {
            return;
        }
        registry.register(alias, (ctx, input, meta) -> invokeHelper(def, ctx, input), def.outputs(), def.metadata());
    }

    private static Object invokeHelper(HelperDefinition def, ExecutionContext ctx, Map<String, Object> input) throws Exception {
        List<Map<String, Object>> steps = COMPOSE_CACHE.computeIfAbsent(def.composePath(), ComposeLoader::loadFromLocalFile);
        Map<String, Object> initial = input == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(input);
        return ComposeRunner.runSteps(ctx, steps, initial, Map.of());
    }

    private static List<HelperDefinition> collectDefinitions() {
        Set<Path> visitedRoots = new LinkedHashSet<>();
        List<HelperDefinition> collected = new ArrayList<>();
        for (Path root : gatherRoots()) {
            Path normalized = normalize(root);
            if (!visitedRoots.add(normalized)) {
                continue;
            }
            collected.addAll(loadDefinitionsFromRoot(normalized));
        }
        collected.sort(Comparator.comparing(HelperDefinition::id));
        return collected;
    }

    private static List<Path> gatherRoots() {
        List<Path> roots = new ArrayList<>();
        roots.add(normalize(Paths.get(".")));
        addPathsFromEnv(roots, "LCOD_COMPONENTS_PATH");
        addPathsFromEnv(roots, "LCOD_COMPONENTS_PATHS");
        addPathsFromEnv(roots, "LCOD_WORKSPACE_PATHS");
        addPathsFromEnv(roots, "LCOD_RESOLVER_PATH");
        addPathsFromEnv(roots, "LCOD_RESOLVER_COMPONENTS_PATH");

        // Useful when running from a mono checkout with sibling repos.
        Path sibling = normalize(Paths.get("..", "lcod-components"));
        if (Files.exists(sibling)) {
            roots.add(sibling);
        }
        Path resolverSibling = normalize(Paths.get("..", "lcod-resolver"));
        if (Files.exists(resolverSibling)) {
            roots.add(resolverSibling);
        }
        return roots;
    }

    private static void addPathsFromEnv(List<Path> roots, String key) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return;
        }
        PATH_SEPARATOR.splitAsStream(raw)
            .map(String::trim)
            .filter(segment -> !segment.isEmpty())
            .map(Paths::get)
            .map(ResolverHelperLoader::normalize)
            .forEach(roots::add);
    }

    private static List<HelperDefinition> loadDefinitionsFromRoot(Path root) {
        if (!Files.exists(root)) {
            return List.of();
        }

        List<HelperDefinition> defs = new ArrayList<>();
        boolean handled = false;
        Path workspaceManifest = root.resolve("workspace.lcp.toml");
        if (Files.isRegularFile(workspaceManifest)) {
            defs.addAll(loadWorkspaceDefinitions(root, workspaceManifest));
            handled = true;
        }

        if (!handled) {
            defs.addAll(loadLegacyDefinitions(root));
        }

        return defs;
    }

    private static List<HelperDefinition> loadWorkspaceDefinitions(Path root, Path workspaceManifest) {
        TomlParseResult workspaceToml = parseToml(workspaceManifest);
        if (workspaceToml == null) {
            return List.of();
        }
        TomlTable workspace = workspaceToml.getTable("workspace");
        if (workspace == null) {
            return List.of();
        }

        Map<String, String> workspaceAliases = readAliasMap(workspace.getTable("scopeAliases"));
        List<String> packages = readStringArray(workspace.getArray("packages"));
        if (packages.isEmpty()) {
            return List.of();
        }

        List<HelperDefinition> defs = new ArrayList<>();
        for (String pkg : packages) {
            Path packageDir = root.resolve("packages").resolve(pkg);
            defs.addAll(loadPackageDefinitions(packageDir, workspaceAliases));
        }
        return defs;
    }

    private static List<HelperDefinition> loadLegacyDefinitions(Path root) {
        List<HelperDefinition> defs = new ArrayList<>();

        Path componentsDir = root.resolve("components");
        if (Files.isDirectory(componentsDir)) {
            defs.addAll(loadComponentsFromDirectory(componentsDir, HelperContext.empty()));
        }

        Path packagesDir = root.resolve("packages");
        if (Files.isDirectory(packagesDir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(packagesDir)) {
                for (Path pkg : stream) {
                    if (Files.isDirectory(pkg)) {
                        defs.addAll(loadPackageDefinitions(pkg, Map.of()));
                    }
                }
            } catch (IOException ignored) {
                // ignore unreadable packages directory
            }
        }

        if (defs.isEmpty() && Files.isDirectory(root)) {
            defs.addAll(loadComponentsFromDirectory(root, HelperContext.empty()));
        }

        return defs;
    }

    private static List<HelperDefinition> loadPackageDefinitions(Path packageDir, Map<String, String> workspaceAliases) {
        Path manifestPath = packageDir.resolve("lcp.toml");
        TomlParseResult manifestToml = parseToml(manifestPath);
        if (manifestToml == null) {
            return List.of();
        }

        Map<String, String> aliasMap = new LinkedHashMap<>(workspaceAliases);
        TomlTable workspaceSection = manifestToml.getTable("workspace");
        aliasMap.putAll(readAliasMap(workspaceSection == null ? null : workspaceSection.getTable("scopeAliases")));

        String version = Optional.ofNullable(manifestToml.getString("version")).orElse("");
        String basePath = deriveBasePath(manifestToml);
        HelperContext context = new HelperContext(basePath, version, aliasMap);

        String componentsDir = workspaceSection != null ? workspaceSection.getString("componentsDir") : null;
        if (componentsDir == null || componentsDir.isBlank()) {
            componentsDir = "components";
        }

        Path componentsPath = packageDir.resolve(componentsDir);
        return loadComponentsFromDirectory(componentsPath, context);
    }

    private static List<HelperDefinition> loadComponentsFromDirectory(Path componentsPath, HelperContext context) {
        if (!Files.isDirectory(componentsPath)) {
            return List.of();
        }

        List<HelperDefinition> defs = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(componentsPath)) {
            stream.filter(Files::isDirectory).forEach(componentDir -> {
                Path manifestPath = componentDir.resolve("lcp.toml");
                Path composePath = componentDir.resolve("compose.yaml");
                if (Files.isRegularFile(manifestPath) && Files.isRegularFile(composePath)) {
                    HelperDefinition def = loadComponentDefinition(componentDir, manifestPath, composePath, context);
                    if (def != null) {
                        defs.add(def);
                    }
                }
            });
        } catch (IOException ignored) {
            // best effort scan; unreadable directories are skipped
        }
        return defs;
    }

    private static HelperDefinition loadComponentDefinition(Path componentDir, Path manifestPath, Path composePath, HelperContext context) {
        TomlParseResult manifestToml = parseToml(manifestPath);
        if (manifestToml == null) {
            return null;
        }
        String rawId = manifestToml.getString("id");
        if (rawId == null || rawId.isBlank()) {
            return null;
        }

        String canonicalId = canonicalizeId(rawId, context);
        if (canonicalId == null || canonicalId.isBlank()) {
            return null;
        }
        List<String> outputs = readTomlKeys(manifestToml.getTable("outputs"));
        List<String> aliases = new ArrayList<>();
        if (!canonicalId.equals(rawId)) {
            aliases.add(rawId);
        }

        ComponentMetadata metadata = ComponentMetadataLoader.fromToml(manifestToml).orElse(null);
        return new HelperDefinition(canonicalId, composePath.toAbsolutePath().normalize(), outputs, aliases, metadata);
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

    private static Map<String, String> readAliasMap(TomlTable table) {
        if (table == null) {
            return Map.of();
        }
        Map<String, String> aliases = new LinkedHashMap<>();
        for (String key : table.keySet()) {
            String value = table.getString(key);
            if (value != null && !value.isBlank()) {
                aliases.put(key, value);
            }
        }
        return aliases;
    }

    private static List<String> readStringArray(TomlArray array) {
        if (array == null || array.isEmpty()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            String value = array.getString(i);
            if (value != null && !value.isBlank()) {
                values.add(value);
            }
        }
        return values;
    }

    private static List<String> readTomlKeys(TomlTable table) {
        if (table == null || table.isEmpty()) {
            return List.of();
        }
        return table.keySet().stream().collect(Collectors.toList());
    }

    private static String deriveBasePath(TomlParseResult manifest) {
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

    private static String canonicalizeId(String raw, HelperContext context) {
        if (raw == null || raw.isBlank()) {
            return null;
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
        String mapped = context.aliasMap().getOrDefault(first, first);

        List<String> parts = new ArrayList<>();
        if (!context.basePath().isEmpty()) {
            parts.add(context.basePath());
        }
        if (!mapped.isEmpty()) {
            parts.add(mapped);
        }
        for (int i = 1; i < segments.length; i++) {
            if (!segments[i].isEmpty()) {
                parts.add(segments[i]);
            }
        }
        String base = String.join("/", parts);
        if (base.isEmpty()) {
            base = normalized.replace('.', '/');
        }
        if (base.isEmpty()) {
            return trimmed;
        }
        String version = context.version().isEmpty() ? "0.0.0" : context.version();
        return "lcod://" + base + "@" + version;
    }

    private static Path normalize(Path path) {
        return path.toAbsolutePath().normalize();
    }

    private record HelperDefinition(String id, Path composePath, List<String> outputs, List<String> aliases, ComponentMetadata metadata) {}

    private record HelperContext(String basePath, String version, Map<String, String> aliasMap) {
        static HelperContext empty() {
            return new HelperContext("", "", Map.of());
        }
    }
}
