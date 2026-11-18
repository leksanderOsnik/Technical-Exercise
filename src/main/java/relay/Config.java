package relay;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * All tunable limits and timeouts in one immutable place.
 *
 * <p>Resolution order for each option: command-line flag ({@code --name=value} or
 * {@code --name value}) &gt; environment variable ({@code RELAY_NAME}) &gt; default.
 */
public record Config(
        String host,
        int port,
        int maxConnections,
        int maxClients,
        int mailboxCapacity,
        long maxStoredBytes,
        int maxBodyBytes,
        int maxFrameBytes,
        int inflightWindow,
        int outboundQueue,
        int dedupeWindow,
        Duration registerTimeout,
        Duration idleTimeout,
        Duration shutdownTimeout) {

    /** Option name -> default value. Insertion-ordered so usage text is stable. */
    private static final Map<String, String> DEFAULTS = new LinkedHashMap<>();

    static {
        DEFAULTS.put("host", "0.0.0.0");
        DEFAULTS.put("port", "7000");
        DEFAULTS.put("max-connections", "1000");
        DEFAULTS.put("max-clients", "10000");
        DEFAULTS.put("mailbox-capacity", "1000");
        DEFAULTS.put("max-stored-bytes", "268435456"); // 256 MiB across all mailboxes
        DEFAULTS.put("max-body-bytes", "16384");
        DEFAULTS.put("max-frame-bytes", "65536");
        DEFAULTS.put("inflight-window", "32");
        DEFAULTS.put("outbound-queue", "256");
        DEFAULTS.put("dedupe-window", "1000"); // acked ids remembered per mailbox; 0 = off
        DEFAULTS.put("register-timeout-ms", "10000");
        DEFAULTS.put("idle-timeout-ms", "60000");
        DEFAULTS.put("shutdown-timeout-ms", "5000");
    }

    public Config {
        requirePositive("max-connections", maxConnections);
        requirePositive("max-clients", maxClients);
        requirePositive("mailbox-capacity", mailboxCapacity);
        requirePositive("max-body-bytes", maxBodyBytes);
        requirePositive("inflight-window", inflightWindow);
        requirePositive("outbound-queue", outboundQueue);
        if (dedupeWindow < 0) {
            throw new IllegalArgumentException("dedupe-window must be >= 0, got " + dedupeWindow);
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be 0-65535, got " + port);
        }
        if (maxFrameBytes <= maxBodyBytes) {
            throw new IllegalArgumentException("max-frame-bytes must be larger than max-body-bytes");
        }
        if (maxStoredBytes < maxBodyBytes) {
            throw new IllegalArgumentException("max-stored-bytes must be at least max-body-bytes");
        }
        requirePositive("register-timeout-ms", registerTimeout);
        requirePositive("idle-timeout-ms", idleTimeout);
    }

    public static Config defaults() {
        return parse(new String[0], Map.of());
    }

    public static Config parse(String[] args, Map<String, String> env) {
        Map<String, String> values = new LinkedHashMap<>(DEFAULTS);
        DEFAULTS.keySet().forEach(name -> {
            String fromEnv = env.get(envName(name));
            if (fromEnv != null) {
                values.put(name, fromEnv);
            }
        });
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument: " + arg);
            }
            String name;
            String value;
            int eq = arg.indexOf('=');
            if (eq > 0) {
                name = arg.substring(2, eq);
                value = arg.substring(eq + 1);
            } else {
                name = arg.substring(2);
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for --" + name);
                }
                value = args[++i];
            }
            if (!DEFAULTS.containsKey(name)) {
                throw new IllegalArgumentException("unknown option: --" + name);
            }
            values.put(name, value);
        }
        return new Config(
                values.get("host"),
                intOpt(values, "port"),
                intOpt(values, "max-connections"),
                intOpt(values, "max-clients"),
                intOpt(values, "mailbox-capacity"),
                longOpt(values, "max-stored-bytes"),
                intOpt(values, "max-body-bytes"),
                intOpt(values, "max-frame-bytes"),
                intOpt(values, "inflight-window"),
                intOpt(values, "outbound-queue"),
                intOpt(values, "dedupe-window"),
                Duration.ofMillis(intOpt(values, "register-timeout-ms")),
                Duration.ofMillis(intOpt(values, "idle-timeout-ms")),
                Duration.ofMillis(intOpt(values, "shutdown-timeout-ms")));
    }

    public static String usage() {
        StringBuilder sb = new StringBuilder("Options (flag / env var / default):\n");
        DEFAULTS.forEach((name, def) -> sb.append(String.format(
                "  --%-22s %-28s %s%n", name, envName(name), def)));
        return sb.toString();
    }

    private static String envName(String option) {
        return "RELAY_" + option.toUpperCase().replace('-', '_');
    }

    private static int intOpt(Map<String, String> values, String name) {
        try {
            return Integer.parseInt(values.get(name).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " must be an integer, got '" + values.get(name) + "'");
        }
    }

    private static long longOpt(Map<String, String> values, String name) {
        try {
            return Long.parseLong(values.get(name).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " must be an integer, got '" + values.get(name) + "'");
        }
    }

    private static void requirePositive(String name, Duration value) {
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }

    private static void requirePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0, got " + value);
        }
    }
}
