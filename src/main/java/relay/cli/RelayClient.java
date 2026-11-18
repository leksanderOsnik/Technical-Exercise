package relay.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Minimal interactive demo client. Prints every server frame and auto-acks DELIVERs.
 *
 * <p>The resume token from REGISTERED is saved to {@code ~/.relay-tokens/<host>_<port>_<name>}
 * and sent on the next start, so quitting and restarting reclaims the same mailbox.
 *
 * <pre>
 * java -cp target/relay.jar relay.cli.RelayClient alice [host] [port]
 *   send bob hello there     -> SEND with a random UUID id
 *   raw {"type":"PING"}      -> send any frame verbatim
 *   quit
 * </pre>
 */
public final class RelayClient {

    private static final ObjectMapper JSON = new ObjectMapper();

    private RelayClient() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: RelayClient <name> [host] [port]");
            System.exit(2);
        }
        String name = args[0];
        String host = args.length > 1 ? args[1] : "localhost";
        int port = args.length > 2 ? Integer.parseInt(args[2]) : 7000;

        Path tokenFile = Path.of(System.getProperty("user.home"), ".relay-tokens",
                host.replaceAll("[^A-Za-z0-9.-]", "_") + "_" + port + "_" + name);

        try (Socket socket = new Socket(host, port)) {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            Thread.ofPlatform().daemon().start(() -> {
                try {
                    String line;
                    while ((line = in.readLine()) != null) {
                        System.out.println("< " + line);
                        JsonNode frame = JSON.readTree(line);
                        if ("REGISTERED".equals(frame.path("type").asText())) {
                            saveToken(tokenFile, frame.path("token").asText());
                        }
                        if ("DELIVER".equals(frame.path("type").asText())) {
                            send(out, Map.of("type", "ACK",
                                    "from", frame.path("from").asText(), "id", frame.path("id").asText()));
                        }
                    }
                    System.out.println("(server closed the connection)");
                } catch (IOException e) {
                    System.out.println("(connection lost: " + e.getMessage() + ")");
                }
                System.exit(0);
            });

            Map<String, String> register = new LinkedHashMap<>(Map.of("type", "REGISTER", "name", name));
            if (Files.exists(tokenFile)) {
                register.put("token", Files.readString(tokenFile).strip());
            }
            send(out, register);
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String line;
            while ((line = stdin.readLine()) != null) {
                line = line.strip();
                if (line.equals("quit")) {
                    break;
                } else if (line.startsWith("send ")) {
                    String[] parts = line.split(" ", 3);
                    if (parts.length < 3) {
                        System.out.println("usage: send <to> <text>");
                        continue;
                    }
                    send(out, Map.of("type", "SEND", "id", UUID.randomUUID().toString(),
                            "to", parts[1], "body", parts[2]));
                } else if (line.startsWith("raw ")) {
                    out.println(line.substring(4));
                } else if (!line.isEmpty()) {
                    System.out.println("commands: send <to> <text> | raw <json> | quit");
                }
            }
        }
    }

    private static void saveToken(Path file, String token) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, token);
        } catch (IOException e) {
            System.out.println("(could not save resume token: " + e.getMessage() + ")");
        }
    }

    private static void send(PrintWriter out, Map<String, String> frame) throws IOException {
        String json = JSON.writeValueAsString(frame);
        System.out.println("> " + json);
        out.println(json);
    }
}
