package work.lcod.kernel.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import picocli.CommandLine;
import work.lcod.kernel.api.CacheMode;
import work.lcod.kernel.api.ComposeTarget;
import work.lcod.kernel.api.LcodRunConfiguration;
import work.lcod.kernel.api.LcodRunner;
import work.lcod.kernel.api.LogLevel;
import work.lcod.kernel.api.RunResult;
import work.lcod.kernel.shared.DurationParser;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.KernelRegistry;
import work.lcod.kernel.runtime.StepMeta;

@CommandLine.Command(
    name = "lcod-run",
    description = "Execute LCOD composes using the Java kernel.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    showDefaultValues = true
)
final class LcodRunCommand implements java.util.concurrent.Callable<Integer> {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ObjectWriter JSON_WRITER = JSON.writerWithDefaultPrettyPrinter();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    private static final Duration CATALOGUE_TTL = Duration.ofHours(24);
    private static final String[] WORKSPACE_MANIFEST_FILES = new String[] {
        "registry/components.std.jsonl",
        "components.std.jsonl",
        "manifest.jsonl",
        "runtime/manifest.jsonl"
    };
    private static final String DEFAULT_CATALOGUE_URL =
        "https://raw.githubusercontent.com/lcod-team/lcod-components/main/registry/components.std.jsonl";
    private static final String DEFAULT_COMPONENTS_REPO = "https://github.com/lcod-team/lcod-components";
    private static final String RESOLVER_COMPONENT_ID = "lcod://resolver/locate_component@0.1.0";
    private static final StepMeta EMPTY_STEP_META = new StepMeta(Map.of(), Map.of(), null);
    private record ManifestMetadata(List<String> inputs, List<String> outputs) {}

    @CommandLine.Option(
        names = {"-c", "--compose"},
        required = true,
        description = "Compose file path or HTTP(S) URL.",
        arity = "1..*"
    )
    private List<String> composePaths = new ArrayList<>();

    @CommandLine.Option(
        names = {"-i", "--input"},
        paramLabel = "PATH|-",
        description = "JSON input file; use '-' to read from stdin (default: {}).",
        defaultValue = CommandLine.Option.NULL_VALUE
    )
    private String input;

    @CommandLine.Option(
        names = "--resolve",
        description = "Force resolver pass even if lcp.lock already exists."
    )
    private boolean forceResolve;

    @CommandLine.Option(
        names = "--lock",
        description = "Lockfile output path (default: <compose-dir>/lcp.lock).",
        defaultValue = CommandLine.Option.NULL_VALUE
    )
    private String lock;

    @CommandLine.Option(
        names = "--cache-dir",
        description = "Location of the execution cache (overrides --global-cache/local default).",
        defaultValue = CommandLine.Option.NULL_VALUE
    )
    private String cacheDir;

    @CommandLine.Option(
        names = {"-g", "--global-cache"},
        description = "Use ~/.lcod/cache instead of <compose-dir>/.lcod/cache."
    )
    private boolean globalCache;

    @CommandLine.Option(
        names = "--log-level",
        description = "Kernel log threshold (trace|debug|info|warn|error|fatal).",
        defaultValue = CommandLine.Option.NULL_VALUE
    )
    private String logLevelRaw;

    @CommandLine.Option(
        names = "--timeout",
        description = "Execution timeout (e.g. 30s, 2m, 1h).",
        defaultValue = CommandLine.Option.NULL_VALUE
    )
    private String timeoutRaw;

    @Override
    public Integer call() throws Exception {
        if (composePaths == null || composePaths.isEmpty()) {
            throw new CommandLine.ParameterException(new CommandLine(this), "At least one --compose value is required.");
        }

        if (composePaths.size() > 1 && lock != null && !lock.isBlank()) {
            throw new CommandLine.ParameterException(
                new CommandLine(this),
                "When using multiple --compose values, --lock is not supported."
            );
        }

        String payload = loadInputPayload();
        Optional<Duration> timeout = DurationParser.parse(timeoutRaw);
        LogLevel logLevel = resolveLogLevel();
        CacheMode cacheMode = determineCacheMode();

        LcodRunner runner = new LcodRunner();
        int exitCode = 0;

        for (String compose : composePaths) {
            ComposeTarget target = detectComposeTarget(compose);
            Path workingDir = determineWorkingDirectory(target);
            Path resolvedLock = resolveLockPath(workingDir);
            Path resolvedCacheDir = resolveCacheDirectory(workingDir);

            Optional<ManifestMetadata> manifest = target.localPath().flatMap(this::loadManifestMetadata);
            Map<String, Object> payloadObject;
            try {
                payloadObject = JSON.readValue(payload, MAP_TYPE);
            } catch (IOException ex) {
                throw new CommandLine.ExecutionException(new CommandLine(this), "Unable to parse input payload: " + ex.getMessage(), ex);
            }
            payloadObject = new LinkedHashMap<>(payloadObject);
            payloadObject.putIfAbsent("projectPath", workingDir.toString());
            Path workspaceRoot = findWorkspaceRoot(workingDir);
            if (workspaceRoot != null) {
                payloadObject.putIfAbsent("projectRoot", workspaceRoot.toString());
            } else {
                payloadObject.putIfAbsent("projectRoot", workingDir.toString());
            }
            Map<String, Object> sanitizedPayload = sanitizeInputPayload(payloadObject, manifest);
            String effectivePayload;
            try {
                effectivePayload = JSON.writeValueAsString(sanitizedPayload);
            } catch (IOException ex) {
                throw new CommandLine.ExecutionException(new CommandLine(this), "Unable to serialize sanitized payload: " + ex.getMessage(), ex);
            }

            LcodRunConfiguration configuration = LcodRunConfiguration.builder()
                .composeTarget(target)
                .workingDirectory(workingDir)
                .lockFile(resolvedLock)
                .cacheDirectory(resolvedCacheDir)
                .cacheMode(cacheMode)
                .forceResolve(forceResolve)
                .inputPayload(effectivePayload)
                .timeout(timeout)
                .logLevel(logLevel)
                .build();

            RunResult result = runner.run(configuration);
            exitCode = Math.max(exitCode, result.status().exitCode());

            Map<String, Object> publicPayload = projectOutputs(result, manifest);
            System.out.println(JSON_WRITER.writeValueAsString(publicPayload));
        }

        return exitCode;
    }

    private ComposeTarget detectComposeTarget(String value) {
        if (value.startsWith("lcod://")) {
            Path localPath = resolveComponentToLocalPath(value);
            return ComposeTarget.forLocal(localPath);
        }
        if (value.startsWith("http://") || value.startsWith("https://")) {
            return ComposeTarget.forRemote(URI.create(value));
        }
        Path path = Paths.get(value).toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            throw new CommandLine.ParameterException(
                new CommandLine(this),
                "Compose file not found: " + path
            );
        }
        return ComposeTarget.forLocal(path);
    }

    private Path determineWorkingDirectory(ComposeTarget target) {
        if (target.isRemote()) {
            return Paths.get("").toAbsolutePath();
        }
        Path local = target.localPath().orElseThrow();
        Path parent = local.getParent();
        Path anchor = parent != null ? parent : local;
        Path workspaceRoot = findWorkspaceRoot(anchor);
        return workspaceRoot != null ? workspaceRoot : anchor;
    }

    private Path resolveLockPath(Path workingDir) {
        if (lock != null) {
            return Paths.get(lock).toAbsolutePath().normalize();
        }
        return workingDir.resolve("lcp.lock").toAbsolutePath().normalize();
    }

    private CacheMode determineCacheMode() {
        if (cacheDir != null) {
            return CacheMode.CUSTOM;
        }
        return globalCache ? CacheMode.GLOBAL : CacheMode.LOCAL;
    }

    private Path resolveCacheDirectory(Path workingDir) {
        if (cacheDir != null) {
            return Paths.get(cacheDir).toAbsolutePath().normalize();
        }
        if (globalCache) {
            Path home = Path.of(System.getProperty("user.home"), ".lcod", "cache");
            return home.toAbsolutePath().normalize();
        }
        return workingDir.resolve(".lcod/cache").toAbsolutePath().normalize();
    }

    private String loadInputPayload() {
        if (input == null || input.isBlank()) {
            return "{}";
        }
        if ("-".equals(input)) {
            String payload = readStdin();
            validateJsonPayload(payload);
            return payload;
        }
        String trimmed = input.trim();
        if (trimmed.startsWith("{")) {
            validateJsonPayload(trimmed);
            return trimmed;
        }
        Path path = Paths.get(input).toAbsolutePath().normalize();
        try {
            String content = Files.readString(path, StandardCharsets.UTF_8);
            validateJsonPayload(content);
            return content;
        } catch (IOException ex) {
            throw new CommandLine.ParameterException(new CommandLine(this), "Cannot read input file: " + path);
        }
    }

    private void validateJsonPayload(String payload) {
        if (payload == null) {
            return;
        }
        String trimmed = payload.trim();
        if (trimmed.isEmpty()) {
            return;
        }
        try {
            var node = JSON.readTree(trimmed);
            if (node != null && !node.isObject()) {
                throw new CommandLine.ParameterException(new CommandLine(this), "JSON payload must be an object");
            }
        } catch (IOException ex) {
            throw new CommandLine.ParameterException(new CommandLine(this), "Invalid JSON payload: " + ex.getMessage());
        }
    }

    private String readStdin() {
        try {
            InputStream stdin = System.in;
            byte[] bytes = stdin.readAllBytes();
            if (bytes.length == 0) {
                return "{}";
            }
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new CommandLine.ExecutionException(new CommandLine(this), "Unable to read stdin: " + ex.getMessage(), ex);
        }
    }

    private Path resolveComponentToLocalPath(String componentId) {
        Objects.requireNonNull(componentId, "componentId");

        Path resolved = resolveViaRegistry(componentId);
        if (resolved != null) {
            return resolved;
        }

        Exception fallbackFailure = null;
        try {
            Path fallbackPath = fallbackResolveComponent(componentId);
            if (!Files.exists(fallbackPath)) {
                throw new IOException("Compose file missing after fallback: " + fallbackPath);
            }
            System.err.printf("Resolved %s via catalogue fallback at %s%n", componentId, fallbackPath);
            return fallbackPath;
        } catch (Exception ex) {
            fallbackFailure = ex;
        }

        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            "Unable to resolve " + componentId + ": " + (fallbackFailure != null ? fallbackFailure.getMessage() : "component not found"),
            fallbackFailure
        );
    }

    private Path resolveViaRegistry(String componentId) {
        try {
            var registry = KernelRegistry.create();
            var ctx = new ExecutionContext(registry);
            @SuppressWarnings("unchecked")
            Map<String, Object> response = (Map<String, Object>) ctx.call(
                RESOLVER_COMPONENT_ID,
                Map.of("componentId", componentId),
                EMPTY_STEP_META
            );
            if (response != null && Boolean.TRUE.equals(response.get("found"))) {
                Object rawResult = response.get("result");
                if (rawResult instanceof Map<?, ?> resultMap) {
                    Path resolved = extractComposePathFromResult((Map<?, ?>) resultMap);
                    if (resolved != null && Files.exists(resolved)) {
                        return resolved;
                    }
                }
            }
        } catch (Exception ex) {
            System.err.printf("resolver locate_component failed for %s: %s%n", componentId, ex.getMessage());
        }
        return null;
    }

    private Path extractComposePathFromResult(Map<?, ?> result) {
        Path candidate = normalizePath(result.get("composePath"));
        if (candidate != null) {
            return candidate;
        }
        Object compose = result.get("compose");
        if (compose instanceof Map<?, ?> composeMap) {
            candidate = normalizePath(((Map<?, ?>) composeMap).get("path"));
            if (candidate != null) {
                return candidate;
            }
        }
        return null;
    }

    private Path fallbackResolveComponent(String componentId) throws IOException, InterruptedException {
        ComponentParts parts = splitComponentId(componentId);
        Path cacheRoot = cacheRootDirectory();
        Files.createDirectories(cacheRoot);

        Path cataloguePath = ensureCatalogueCached(cacheRoot);
        Map<String, Object> entry = findCatalogueEntry(cataloguePath, componentId);
        if (entry == null) {
            throw new IOException("Component " + componentId + " not found in default catalogue");
        }
        Optional<Path> manifestBase = catalogueBaseDir(cataloguePath);
        Optional<Path> localCompose = resolveLocalManifestFile(manifestBase, entry.get("compose"));
        if (localCompose.isPresent()) {
            return localCompose.get();
        }

        String safeKey = sanitizeComponentKey(parts.key());
        Path componentDir = cacheRoot
            .resolve("components")
            .resolve(safeKey)
            .resolve(parts.version());
        Files.createDirectories(componentDir);

        Path composePath = componentDir.resolve("compose.yaml");
        if (!Files.exists(composePath)) {
            String composeUrl = buildComponentUrl(entry, entry.get("compose"));
            if (composeUrl == null || composeUrl.isBlank()) {
                throw new IOException("Catalogue entry for " + componentId + " missing compose path");
            }
            downloadUrlToPath(composeUrl, composePath);
        }

        String lcpPath = extractLcpPath(entry.get("lcp"));
        if (lcpPath != null && !lcpPath.isBlank()) {
            Path target = componentDir.resolve("lcp.toml");
            if (!Files.exists(target)) {
                Optional<Path> localLcp = resolveLocalManifestFile(manifestBase, lcpPath);
                if (localLcp.isPresent()) {
                    Files.copy(localLcp.get(), target);
                } else {
                    String lcpUrl = buildComponentUrl(entry, lcpPath);
                    if (lcpUrl != null && !lcpUrl.isBlank()) {
                        downloadUrlToPath(lcpUrl, target);
                    }
                }
            }
        }

        return composePath.toAbsolutePath().normalize();
    }

    private ComponentParts splitComponentId(String componentId) {
        if (!componentId.startsWith("lcod://")) {
            throw new IllegalArgumentException("component id must start with lcod://");
        }
        String trimmed = componentId.substring("lcod://".length());
        int at = trimmed.indexOf('@');
        if (at < 0) {
            if (trimmed.isBlank()) {
                throw new IllegalArgumentException("component id missing identifier");
            }
            return new ComponentParts(trimmed, "0.0.0");
        }
        String key = trimmed.substring(0, at);
        String version = trimmed.substring(at + 1);
        if (key.isBlank()) {
            throw new IllegalArgumentException("component id missing identifier");
        }
        if (version == null || version.isBlank()) {
            version = "0.0.0";
        }
        return new ComponentParts(key, version);
    }

    private String sanitizeComponentKey(String key) {
        return key.replaceAll("[^A-Za-z0-9]", "_");
    }

    private Path cacheRootDirectory() {
        String home = System.getProperty("user.home");
        if (home == null || home.isBlank()) {
            throw new IllegalStateException("Unable to determine user home directory");
        }
        return Paths.get(home, ".lcod", "cache").toAbsolutePath().normalize();
    }

    private Optional<Path> manifestFromRoot(String root) {
        if (root == null || root.isBlank()) {
            return Optional.empty();
        }
        Path base = Paths.get(root);
        Path direct = base.resolve("manifest.jsonl");
        if (Files.isRegularFile(direct)) {
            return Optional.of(direct);
        }
        Path nested = base.resolve("runtime").resolve("manifest.jsonl");
        if (Files.isRegularFile(nested)) {
            return Optional.of(nested);
        }
        return Optional.empty();
    }

    private Optional<Path> catalogueBaseDir(Path manifest) {
        Path parent = manifest.getParent();
        if (parent == null) {
            return Optional.empty();
        }
        Path resolvedParent = parent;
        Path fileName = parent.getFileName();
        if (fileName != null && "registry".equalsIgnoreCase(fileName.toString())) {
            Path grand = parent.getParent();
            if (grand != null) {
                resolvedParent = grand;
            }
        }
        return Optional.of(resolvedParent);
    }

    private Optional<Path> resolveLocalManifestFile(Optional<Path> baseDir, Object manifestEntry) {
        if (baseDir.isEmpty() || manifestEntry == null) {
            return Optional.empty();
        }
        String raw = manifestEntry.toString().trim();
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        String cleaned = raw.startsWith("./") ? raw.substring(2) : raw;
        Path candidate = baseDir.get().resolve(cleaned).normalize();
        return Files.isRegularFile(candidate) ? Optional.of(candidate) : Optional.empty();
    }

    private List<String> splitEnvPaths(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        String[] segments = raw.split(File.pathSeparator);
        List<String> values = new ArrayList<>(segments.length);
        for (String segment : segments) {
            if (segment != null) {
                String trimmed = segment.trim();
                if (!trimmed.isEmpty()) {
                    values.add(trimmed);
                }
            }
        }
        return values;
    }

    private List<Path> workspaceManifestCandidates() {
        LinkedHashSet<Path> candidates = new LinkedHashSet<>();

        LinkedHashSet<String> roots = new LinkedHashSet<>();
        String cwd = System.getProperty("user.dir");
        if (cwd != null && !cwd.isBlank()) {
            roots.add(cwd);
        }
        for (String envVar : List.of("LCOD_WORKSPACE_PATHS", "LCOD_COMPONENTS_PATHS", "LCOD_COMPONENTS_PATH")) {
            String raw = System.getenv(envVar);
            if (raw != null && !raw.isBlank()) {
                roots.addAll(splitEnvPaths(raw));
            }
        }

        List<String> explicit = null;
        String rawExplicit = System.getenv("LCOD_COMPONENTS_MANIFESTS");
        if (rawExplicit != null && !rawExplicit.isBlank()) {
            explicit = splitEnvPaths(rawExplicit);
        }

        for (String root : roots) {
            if (root == null || root.isBlank()) {
                continue;
            }
            Path base = Paths.get(root);
            for (String relative : WORKSPACE_MANIFEST_FILES) {
                candidates.add(base.resolve(relative));
            }
        }

        if (explicit != null) {
            for (String entry : explicit) {
                candidates.add(Paths.get(entry));
            }
        }

        return new ArrayList<>(candidates);
    }

    private Optional<Path> runtimeManifestFromEnv() {
        return Stream.of(
                System.getenv("LCOD_HOME"),
                System.getenv("SPEC_REPO_PATH"),
                System.getenv("LCOD_COMPONENTS_PATH")
            )
            .filter(Objects::nonNull)
            .map(this::manifestFromRoot)
            .flatMap(Optional::stream)
            .findFirst();
    }

    private Path ensureCatalogueCached(Path cacheRoot) throws IOException, InterruptedException {
        for (Path candidate : workspaceManifestCandidates()) {
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }

        Optional<Path> localManifest = runtimeManifestFromEnv();
        if (localManifest.isPresent()) {
            return localManifest.get();
        }

        Path catalogueDir = cacheRoot.resolve("catalogues");
        Files.createDirectories(catalogueDir);
        Path cataloguePath = catalogueDir.resolve("components.std.jsonl");
        boolean refresh = true;
        if (Files.exists(cataloguePath)) {
            Instant modified = Files.getLastModifiedTime(cataloguePath).toInstant();
            Duration age = Duration.between(modified, Instant.now());
            if (!age.isNegative() && age.compareTo(CATALOGUE_TTL) <= 0) {
                refresh = false;
            }
        }
        if (refresh) {
            downloadUrlToPath(DEFAULT_CATALOGUE_URL, cataloguePath);
        }
        return cataloguePath;
    }

    private Map<String, Object> findCatalogueEntry(Path cataloguePath, String componentId) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(cataloguePath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                try {
                    Map<String, Object> entry = JSON.readValue(trimmed, MAP_TYPE);
                    if (componentId.equals(entry.get("id"))) {
                        return entry;
                    }
                } catch (IOException ex) {
                    // ignore malformed line and continue
                }
            }
        }
        return null;
    }

    private String buildComponentUrl(Map<String, Object> entry, Object manifestPath) {
        if (manifestPath == null) {
            return null;
        }
        String pathValue = manifestPath.toString().trim();
        if (pathValue.isEmpty()) {
            return null;
        }
        if (pathValue.startsWith("./")) {
            pathValue = pathValue.substring(2);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> origin = entry.containsKey("origin") && entry.get("origin") instanceof Map
            ? (Map<String, Object>) entry.get("origin")
            : Map.of();
        String sourceRepo = Optional.ofNullable(origin.get("source_repo"))
            .or(() -> Optional.ofNullable(origin.get("sourceRepo")))
            .map(Object::toString)
            .filter(s -> !s.isBlank())
            .orElse(DEFAULT_COMPONENTS_REPO);
        String commit = Optional.ofNullable(origin.get("commit"))
            .map(Object::toString)
            .filter(s -> !s.isBlank())
            .orElse("main");
        String rawBase = repoToRawBase(sourceRepo, commit);
        return rawBase + pathValue;
    }

    private String extractLcpPath(Object field) {
        if (field instanceof String str) {
            return str;
        }
        if (field instanceof Map<?, ?> map) {
            Object pathValue = map.get("path");
            if (pathValue instanceof String str && !str.isBlank()) {
                return str;
            }
            Object urlValue = map.get("url");
            if (urlValue instanceof String str && !str.isBlank()) {
                return str;
            }
        }
        return null;
    }

    private String repoToRawBase(String repo, String commit) {
        if (repo == null || repo.isBlank()) {
            throw new IllegalArgumentException("Missing repository URL in catalogue entry");
        }
        String normalized = repo;
        if (normalized.startsWith("https://github.com/")) {
            normalized = normalized.substring("https://github.com/".length());
        }
        normalized = normalized.replaceAll("/+$", "");
        if (!normalized.contains("/")) {
            throw new IllegalArgumentException("Unsupported repository URL: " + repo);
        }
        return "https://raw.githubusercontent.com/" + normalized + "/" + commit + "/";
    }

    private void downloadUrlToPath(String url, Path target) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();
        HttpResponse<byte[]> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new IOException("Failed to download " + url + ": HTTP " + status);
        }
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(target, response.body());
    }

    private Path normalizePath(Object value) {
        if (value instanceof String str && !str.isBlank()) {
            return Paths.get(str).toAbsolutePath().normalize();
        }
        return null;
    }

    private record ComponentParts(String key, String version) {}

    private LogLevel resolveLogLevel() {
        String candidate = logLevelRaw;
        if (candidate == null || candidate.isBlank()) {
            candidate = System.getenv("LCOD_LOG_LEVEL");
        }
        if (candidate == null || candidate.isBlank()) {
            candidate = "fatal";
        }
        return LogLevel.from(candidate);
    }

    private Optional<ManifestMetadata> loadManifestMetadata(Path composePath) {
        if (composePath == null) {
            return Optional.empty();
        }
        Path manifestPath = composePath.getParent().resolve("lcp.toml");
        if (!Files.isRegularFile(manifestPath)) {
            return Optional.empty();
        }
        try {
            String raw = Files.readString(manifestPath, StandardCharsets.UTF_8);
            TomlParseResult result = Toml.parse(raw);
            if (result.hasErrors()) {
                return Optional.empty();
            }
            List<String> inputs = extractTomlKeys(result.getTable("inputs"));
            List<String> outputs = extractTomlKeys(result.getTable("outputs"));
            if (inputs.isEmpty() && outputs.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new ManifestMetadata(inputs, outputs));
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    private List<String> extractTomlKeys(TomlTable table) {
        if (table == null) {
            return List.of();
        }
        List<String> keys = new ArrayList<>();
        for (String key : table.keySet()) {
            keys.add(key);
        }
        return keys;
    }

    private Map<String, Object> sanitizeInputPayload(Map<String, Object> original, Optional<ManifestMetadata> manifest) {
        if (manifest.isEmpty() || manifest.get().inputs().isEmpty()) {
            return new LinkedHashMap<>(original);
        }
        Map<String, Object> sanitized = new LinkedHashMap<>();
        for (String key : manifest.get().inputs()) {
            if (original.containsKey(key)) {
                sanitized.put(key, original.get(key));
            }
        }
        return sanitized;
    }

    private Path findWorkspaceRoot(Path start) {
        if (start == null) {
            return null;
        }
        Path current = start.toAbsolutePath();
        for (int depth = 0; depth < 12 && current != null; depth++) {
            if (Files.isRegularFile(current.resolve("workspace.lcp.toml"))) {
                return current;
            }
            current = current.getParent();
        }
        return null;
    }

    private Map<String, Object> projectOutputs(RunResult result, Optional<ManifestMetadata> manifest) {
        if (result.status() != RunResult.Status.SUCCESS) {
            Map<String, Object> errorPayload = new LinkedHashMap<>();
            Object error = result.metadata().get("error");
            if (error == null) {
                errorPayload.put("error", Map.of("message", "Kernel execution failed"));
            } else if (error instanceof Map<?, ?> map) {
                Map<String, Object> converted = new LinkedHashMap<>();
                map.forEach((key, value) -> converted.put(String.valueOf(key), value));
                errorPayload.put("error", converted);
            } else {
                errorPayload.put("error", error);
            }
            return errorPayload;
        }

        Map<String, Object> state = new LinkedHashMap<>();
        Object rawState = result.metadata().get("result");
        if (rawState instanceof Map<?, ?> map) {
            map.forEach((key, value) -> state.put(String.valueOf(key), value));
        } else if (rawState != null) {
            state.put("value", rawState);
        }

        if (manifest.isEmpty() || manifest.get().outputs().isEmpty()) {
            return state;
        }

        Map<String, Object> projected = new LinkedHashMap<>();
        for (String key : manifest.get().outputs()) {
            projected.put(key, state.containsKey(key) ? state.get(key) : null);
        }
        return projected;
    }
}
