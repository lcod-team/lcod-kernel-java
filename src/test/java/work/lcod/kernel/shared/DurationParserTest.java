package work.lcod.kernel.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DurationParserTest {
    @Test
    void parsesSeconds() {
        Optional<Duration> duration = DurationParser.parse("30s");
        assertTrue(duration.isPresent());
        assertEquals(Duration.ofSeconds(30), duration.get());
    }

    @Test
    void parsesMinutes() {
        Optional<Duration> duration = DurationParser.parse("2m");
        assertTrue(duration.isPresent());
        assertEquals(Duration.ofMinutes(2), duration.get());
    }

    @Test
    void parsesHours() {
        Optional<Duration> duration = DurationParser.parse("1h");
        assertTrue(duration.isPresent());
        assertEquals(Duration.ofHours(1), duration.get());
    }

    @Test
    void parsesMillisecondsByDefault() {
        Optional<Duration> duration = DurationParser.parse("1500");
        assertTrue(duration.isPresent());
        assertEquals(Duration.ofMillis(1500), duration.get());
    }

    @Test
    void handlesZero() {
        Optional<Duration> duration = DurationParser.parse("0");
        assertTrue(duration.isPresent());
        assertEquals(Duration.ZERO, duration.get());
    }
}
