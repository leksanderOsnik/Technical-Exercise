package relay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Blocking protocol client for tests. Every read is bounded by {@link #TIMEOUT}.
 *
 * <p>Like a real client, it remembers the resume token from each REGISTERED reply
 * (per server port and name) and sends it on later REGISTERs of the same name.
 */
final class TestClient implements AutoCloseable {

    static final Duration TIMEOUT = Duration.ofSeconds(2);

    /** "port/name" -> resume token. Ports are ephemeral per test, so tests never share entries. */
    private static final Map<String, String> TOKENS = new ConcurrentHashMap<>();

    private final Socket socket;
    private final int port;
    private final BufferedReader in;
    private final OutputStream out;

    private TestClient(Socket socket, int port) throws IOException {
        this.socket = socket;
        this.port = port;
        socket.setSoTimeout((int) TIMEOUT.toMillis());
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.out = socket.getOutputStream();
    }

    static TestClient connect(int port) throws IOException {
        return connect(port, -1);
    }

    /** @param receiveBuffer SO_RCVBUF in bytes, or -1 for the OS default */
    static TestClient connect(int port, int receiveBuffer) throws IOException {
        Socket s = new Socket();
        if (receiveBuffer > 0) {
            s.setReceiveBufferSize(receiveBuffer); // must be set before connect
        }
        s.connect(new InetSocketAddress("localhost", port), (int) TIMEOUT.toMillis());
        return new TestClient(s, port);
    }

    static TestClient register(int port, String name) throws IOException {
        TestClient c = connect(port);
        c.send(c.registerFrame(name));
        c.expect("REGISTERED");
        return c;
    }

    /** A REGISTER frame carrying the remembered token for this name, if there is one. */
    ObjectNode registerFrame(String name) {
        ObjectNode f = frame("REGISTER").put("name", name);
        String token = TOKENS.get(port + "/" + name);
        if (token != null) {
            f.put("token", token);
        }
        return f;
    }

    static ObjectNode frame(String type) {
        return Protocol.JSON.createObjectNode().put("type", type);
    }

    void send(ObjectNode frame) throws IOException {
        sendRaw(Protocol.JSON.writeValueAsString(frame));
    }

    void sendRaw(String line) throws IOException {
        out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /** Sends a SEND and returns the SEND_RESULT. */
    JsonNode sendMessage(String to, String id, String body) throws IOException {
        send(frame("SEND").put("to", to).put("id", id).put("body", body));
        JsonNode result = expect("SEND_RESULT");
        assertEquals(id, result.path("id").asText());
        return result;
    }

    /** Sends an ACK and returns the ACK_RESULT status (OK or STALE). */
    String ack(String from, String id) throws IOException {
        send(frame("ACK").put("from", from).put("id", id));
        return expect("ACK_RESULT").path("status").asText();
    }

    JsonNode next() throws IOException {
        String line;
        try {
            line = in.readLine();
        } catch (SocketTimeoutException e) {
            throw new AssertionError("no frame within " + TIMEOUT.toMillis() + " ms");
        }
        if (line == null) {
            throw new AssertionError("server closed the connection");
        }
        JsonNode frame = Protocol.JSON.readTree(line);
        if ("REGISTERED".equals(frame.path("type").asText())) {
            TOKENS.put(port + "/" + frame.path("name").asText(), frame.path("token").asText());
        }
        return frame;
    }

    JsonNode expect(String type) throws IOException {
        JsonNode frame = next();
        assertEquals(type, frame.path("type").asText(), () -> "unexpected frame: " + frame);
        return frame;
    }

    JsonNode expectError(String code) throws IOException {
        JsonNode frame = expect("ERROR");
        assertEquals(code, frame.path("code").asText(), () -> "unexpected error: " + frame);
        return frame;
    }

    /** Asserts the server closes the connection (EOF or reset) within the timeout. */
    void expectClosed() throws IOException {
        try {
            String line = in.readLine();
            if (line != null) {
                fail("expected connection close, got frame: " + line);
            }
        } catch (SocketTimeoutException e) {
            fail("connection not closed within " + TIMEOUT.toMillis() + " ms");
        } catch (IOException e) {
            // connection reset also counts as closed
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
