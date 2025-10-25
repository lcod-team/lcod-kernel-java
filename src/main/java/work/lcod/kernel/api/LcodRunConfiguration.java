package work.lcod.kernel.api;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable configuration passed to the Java kernel when running a compose.
 */
public record LcodRunConfiguration(
    ComposeTarget composeTarget,
    Path workingDirectory,
    Path lockFile,
    Path cacheDirectory,
    CacheMode cacheMode,
    boolean forceResolve,
    String inputPayload,
    Optional<Duration> timeout,
    LogLevel logLevel
) {
    public LcodRunConfiguration {
        Objects.requireNonNull(composeTarget, "composeTarget");
        Objects.requireNonNull(workingDirectory, "workingDirectory");
        Objects.requireNonNull(lockFile, "lockFile");
        Objects.requireNonNull(cacheDirectory, "cacheDirectory");
        Objects.requireNonNull(cacheMode, "cacheMode");
        Objects.requireNonNull(inputPayload, "inputPayload");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(logLevel, "logLevel");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private ComposeTarget composeTarget;
        private Path workingDirectory;
        private Path lockFile;
        private Path cacheDirectory;
        private CacheMode cacheMode = CacheMode.LOCAL;
        private boolean forceResolve;
        private String inputPayload = "{}";
        private Optional<Duration> timeout = Optional.empty();
        private LogLevel logLevel = LogLevel.FATAL;

        public Builder composeTarget(ComposeTarget composeTarget) {
            this.composeTarget = composeTarget;
            return this;
        }

        public Builder workingDirectory(Path workingDirectory) {
            this.workingDirectory = workingDirectory;
            return this;
        }

        public Builder lockFile(Path lockFile) {
            this.lockFile = lockFile;
            return this;
        }

        public Builder cacheDirectory(Path cacheDirectory) {
            this.cacheDirectory = cacheDirectory;
            return this;
        }

        public Builder cacheMode(CacheMode cacheMode) {
            this.cacheMode = cacheMode;
            return this;
        }

        public Builder forceResolve(boolean forceResolve) {
            this.forceResolve = forceResolve;
            return this;
        }

        public Builder inputPayload(String inputPayload) {
            this.inputPayload = inputPayload;
            return this;
        }

        public Builder timeout(Optional<Duration> timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder logLevel(LogLevel logLevel) {
            this.logLevel = logLevel;
            return this;
        }

        public LcodRunConfiguration build() {
            return new LcodRunConfiguration(
                composeTarget,
                workingDirectory,
                lockFile,
                cacheDirectory,
                cacheMode,
                forceResolve,
                inputPayload,
                timeout,
                logLevel
            );
        }
    }
}
