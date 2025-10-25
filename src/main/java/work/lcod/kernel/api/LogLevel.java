package work.lcod.kernel.api;

import java.util.Locale;

/**
 * Kernel log levels compatible with the Rust/Node runners.
 */
public enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL;

    public static LogLevel from(String value) {
        if (value == null || value.isBlank()) {
            return FATAL;
        }
        try {
            return LogLevel.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported log level: " + value);
        }
    }
}
