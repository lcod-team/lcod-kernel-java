package work.lcod.kernel.shared;

import java.time.Duration;
import java.util.Locale;
import java.util.Optional;

/**
 * Small helper to parse user-friendly durations (e.g. {@code 30s}, {@code 2m}, {@code 5h}).
 */
public final class DurationParser {
    private DurationParser() {}

    public static Optional<Duration> parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        String trimmed = raw.trim().toLowerCase(Locale.ROOT);
        if ("0".equals(trimmed)) {
            return Optional.of(Duration.ZERO);
        }
        long multiplier = 1L;
        if (trimmed.endsWith("ms")) {
            trimmed = trimmed.substring(0, trimmed.length() - 2);
            multiplier = 1L;
        } else if (trimmed.endsWith("s")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
            multiplier = 1_000L;
        } else if (trimmed.endsWith("m")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
            multiplier = 60_000L;
        } else if (trimmed.endsWith("h")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
            multiplier = 3_600_000L;
        }
        long value = Long.parseLong(trimmed);
        return Optional.of(Duration.ofMillis(value * multiplier));
    }
}
