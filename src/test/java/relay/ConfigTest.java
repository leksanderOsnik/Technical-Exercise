package relay;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigTest {

    @Test
    void flagsOverrideEnvOverrideDefaults() {
        Config c = Config.parse(new String[]{"--port", "9000"},
                Map.of("RELAY_PORT", "8000", "RELAY_MAILBOX_CAPACITY", "5"));
        assertEquals(9000, c.port());
        assertEquals(5, c.mailboxCapacity());
        assertEquals(32, c.inflightWindow());
    }

    @Test
    void rejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> Config.parse(new String[]{"--nope=1"}, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> Config.parse(new String[]{"--port=abc"}, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> Config.parse(new String[]{"--max-body-bytes=100", "--max-frame-bytes=100"}, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> Config.parse(new String[]{"--max-stored-bytes=10", "--max-body-bytes=100"}, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> Config.parse(new String[]{"--register-timeout-ms=0"}, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> Config.parse(new String[]{"--dedupe-window=-1"}, Map.of()));
        assertEquals(0, Config.parse(new String[]{"--dedupe-window=0"}, Map.of()).dedupeWindow());
    }
}
