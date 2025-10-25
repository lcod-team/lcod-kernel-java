package work.lcod.kernel.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import picocli.CommandLine;
import work.lcod.kernel.api.CacheMode;
import work.lcod.kernel.api.ComposeTarget;
import work.lcod.kernel.api.LcodRunConfiguration;
import work.lcod.kernel.api.LcodRunner;
import work.lcod.kernel.api.LogLevel;
import work.lcod.kernel.api.RunResult;
import work.lcod.kernel.shared.DurationParser;

@CommandLine.Command(
    name = "lcod-run",
    description = "Execute LCOD composes using the Java kernel.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    showDefaultValues = true
)
final class LcodRunCommand implements java.util.concurrent.Callable<Integer> {
    @CommandLine.Option(
        names = {"-c", "--compose"},
        required = true,
        description = "Compose file path or HTTP(S) URL."
    )
    private String compose;

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
        ComposeTarget target = detectComposeTarget(compose);
        Path workingDir = determineWorkingDirectory(target);

        Path resolvedLock = resolveLockPath(workingDir);
        Path resolvedCacheDir = resolveCacheDirectory(workingDir);
        CacheMode cacheMode = determineCacheMode();
        String payload = loadInputPayload();

        Optional<Duration> timeout = DurationParser.parse(timeoutRaw);
        LogLevel logLevel = resolveLogLevel();

        LcodRunConfiguration configuration = LcodRunConfiguration.builder()
            .composeTarget(target)
            .workingDirectory(workingDir)
            .lockFile(resolvedLock)
            .cacheDirectory(resolvedCacheDir)
            .cacheMode(cacheMode)
            .forceResolve(forceResolve)
            .inputPayload(payload)
            .timeout(timeout)
            .logLevel(logLevel)
            .build();

        RunResult result = new LcodRunner().run(configuration);
        System.out.println(result.toPrettyJson());
        return result.status().exitCode();
    }

    private ComposeTarget detectComposeTarget(String value) {
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
        Path parent = target.localPath().get().getParent();
        return parent != null ? parent : target.localPath().get();
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
            return readStdin();
        }
        Path path = Paths.get(input).toAbsolutePath().normalize();
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new CommandLine.ParameterException(new CommandLine(this), "Cannot read input file: " + path);
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
}
