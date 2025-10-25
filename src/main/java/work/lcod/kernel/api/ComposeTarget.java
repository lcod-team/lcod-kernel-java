package work.lcod.kernel.api;

import java.net.URI;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents the compose argument (local file or remote URL).
 */
public record ComposeTarget(Optional<Path> localPath, Optional<URI> remoteUri) {
    public ComposeTarget {
        Objects.requireNonNull(localPath, "localPath");
        Objects.requireNonNull(remoteUri, "remoteUri");
        if (localPath.isEmpty() && remoteUri.isEmpty()) {
            throw new IllegalArgumentException("Either localPath or remoteUri must be present.");
        }
    }

    public static ComposeTarget forLocal(Path path) {
        return new ComposeTarget(Optional.of(path), Optional.empty());
    }

    public static ComposeTarget forRemote(URI uri) {
        return new ComposeTarget(Optional.empty(), Optional.of(uri));
    }

    public boolean isRemote() {
        return remoteUri.isPresent();
    }

    public String display() {
        return localPath.map(Path::toString).or(() -> remoteUri.map(URI::toString)).orElse("unknown");
    }
}
