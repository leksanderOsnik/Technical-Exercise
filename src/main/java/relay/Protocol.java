package relay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Pattern;

/**
 * Wire protocol: newline-delimited JSON. Every frame is one JSON object on one line
 * with a {@code "type"} field. JSON escapes newlines inside strings, so a raw '\n'
 * byte always terminates a frame.
 *
 * <pre>
 * client -> server   REGISTER {name, token?}
 *                    SEND     {id, to, body}
 *                    ACK      {from, id}
 *                    PING     {}
 * server -> client   REGISTERED  {name, pending, token}
 *                    SEND_RESULT {id, status: ACCEPTED|REJECTED, duplicate?, code?, message?}
 *                    DELIVER     {from, id, body}
 *                    ACK_RESULT  {from, id, status: OK|STALE}
 *                    PONG        {}
 *                    ERROR       {code, message}
 *                    SHUTDOWN    {}
 * </pre>
 */
final class Protocol {

    static final ObjectMapper JSON = new ObjectMapper();

    static final int MAX_ID_LENGTH = 128;
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    enum ErrorCode {
        /** Frame is not valid JSON, has no/unknown type, or misses required fields. */
        BAD_REQUEST,
        /** SEND/ACK attempted before a successful REGISTER on this connection. */
        NOT_REGISTERED,
        /** Name does not match [A-Za-z0-9_-]{1,64}. */
        INVALID_NAME,
        /** REGISTER of an existing name without its resume token, or with the wrong one. */
        INVALID_TOKEN,
        /** SEND to a name that has never registered. */
        UNKNOWN_RECIPIENT,
        /** Recipient mailbox is at capacity; sender should retry later. */
        MAILBOX_FULL,
        /** Server-wide max-stored-bytes reached; sender should retry later. */
        STORAGE_FULL,
        /** Decoded body exceeds max-body-bytes. */
        MESSAGE_TOO_LARGE,
        /** Raw line exceeds max-frame-bytes; connection is closed (cannot resync). */
        FRAME_TOO_LARGE,
        /** max-connections reached; connection is closed. */
        SERVER_BUSY,
        /** max-clients distinct identities already registered. */
        TOO_MANY_CLIENTS,
        /** Another connection registered the same name; this one is closed. */
        SESSION_REPLACED,
        /** No frame received within idle-timeout; connection is closed. */
        IDLE_TIMEOUT,
        /** No successful REGISTER within register-timeout; connection is closed. */
        REGISTER_TIMEOUT
    }

    /** Thrown when an inbound frame cannot be understood. */
    static final class ProtocolException extends Exception {
        final ErrorCode code;

        ProtocolException(ErrorCode code, String message) {
            super(message);
            this.code = code;
        }
    }

    private Protocol() {
    }

    static JsonNode parse(String line) throws ProtocolException {
        JsonNode node;
        try {
            node = JSON.readTree(line);
        } catch (JsonProcessingException e) {
            throw new ProtocolException(ErrorCode.BAD_REQUEST, "frame is not valid JSON");
        }
        if (node == null || !node.isObject()) {
            throw new ProtocolException(ErrorCode.BAD_REQUEST, "frame must be a JSON object");
        }
        return node;
    }

    /** Returns the field as a string, or null if absent or not a JSON string. */
    static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v != null && v.isTextual() ? v.asText() : null;
    }

    static boolean validName(String name) {
        return name != null && NAME.matcher(name).matches();
    }

    static boolean validId(String id) {
        return id != null && !id.isEmpty() && id.length() <= MAX_ID_LENGTH;
    }

    // ---- outbound frame builders -------------------------------------------------

    static String registered(String name, int pending, String token) {
        return encode(frame("REGISTERED").put("name", name).put("pending", pending).put("token", token));
    }

    static String sendAccepted(String id, boolean duplicate) {
        return encode(frame("SEND_RESULT").put("id", id).put("status", "ACCEPTED").put("duplicate", duplicate));
    }

    static String sendRejected(String id, ErrorCode code, String message) {
        return encode(frame("SEND_RESULT").put("id", id).put("status", "REJECTED")
                .put("code", code.name()).put("message", message));
    }

    static String deliver(Message m) {
        return encode(frame("DELIVER").put("from", m.from()).put("id", m.id()).put("body", m.body()));
    }

    static String ackResult(String from, String id, boolean removed) {
        return encode(frame("ACK_RESULT").put("from", from).put("id", id).put("status", removed ? "OK" : "STALE"));
    }

    static String pong() {
        return encode(frame("PONG"));
    }

    static String shutdown() {
        return encode(frame("SHUTDOWN"));
    }

    static String error(ErrorCode code, String message) {
        return encode(frame("ERROR").put("code", code.name()).put("message", message));
    }

    private static ObjectNode frame(String type) {
        return JSON.createObjectNode().put("type", type);
    }

    private static String encode(ObjectNode node) {
        try {
            return JSON.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode frame", e); // not reachable for ObjectNode
        }
    }
}
