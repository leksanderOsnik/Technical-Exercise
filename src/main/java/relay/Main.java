package relay;

import java.util.Arrays;

/** Server entry point: {@code java -jar target/relay.jar [--option=value ...]}. */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        // One-line log format; must be set before the first logger is created.
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tFT%1$tT.%1$tL %4$-7s [%3$s] %5$s%6$s%n");

        if (Arrays.asList(args).contains("--help")) {
            System.out.println("Usage: java -jar relay.jar [options]\n" + Config.usage());
            return;
        }
        Config config;
        try {
            config = Config.parse(args, System.getenv());
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage() + "\n\n" + Config.usage());
            System.exit(2);
            return;
        }

        RelayServer server = new RelayServer(config);
        int port = server.start();
        System.getLogger("relay").log(System.Logger.Level.INFO,
                "relay listening on " + config.host() + ":" + port + " " + config);

        // SIGINT/SIGTERM (Ctrl-C, docker stop) -> graceful shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "relay-shutdown"));
        server.awaitTermination();
    }
}
