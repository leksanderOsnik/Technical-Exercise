package relay;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static relay.TestClient.frame;

/**
 * End-to-end tests over real TCP against a server bound to an ephemeral port.
 * Each test maps to a requirement in APPROACH.md. All waits are bounded.
 */
@Timeout(15)
class RelayIntegrationTest {

    private RelayServer server;
    private int port;

    private void startServer(String... args) throws IOException {
        String[] withPort = new String[args.length + 1];
        withPort[0] = "--port=0";
        System.arraycopy(args, 0, withPort, 1, args.length);
        server = new RelayServer(Config.parse(withPort, Map.of()));
        port = server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    // Scenario 1-4: register, send, accept, deliver, explicit ack.
    @Test
    void registerSendDeliverAck() throws IOException {
        startServer();
        try (TestClient alice = TestClient.register(port, "alice");
             TestClient bob = TestClient.register(port, "bob")) {

            JsonNode result = alice.sendMessage("bob", "m1", "hello");
            assertEquals("ACCEPTED", result.path("status").asText());
            assertFalse(result.path("duplicate").asBoolean());

            JsonNode delivered = bob.expect("DELIVER");
            assertEquals("alice", delivered.path("from").asText());
            assertEquals("m1", delivered.path("id").asText());
            assertEquals("hello", delivered.path("body").asText());

            assertEquals("OK", bob.ack("alice", "m1"));
            assertEquals("STALE", bob.ack("alice", "m1"), "repeated ack is harmless");

            // Either client can send to the other.
            bob.sendMessage("alice", "r1", "hi back");
            assertEquals("r1", alice.expect("DELIVER").path("id").asText());
        }
    }

    // Scenario 5-6: offline retention, reconnect with same name, FIFO order.
    @Test
    void offlineMessagesAreDeliveredInOrderAfterReconnect() throws IOException {
        startServer();
        try (TestClient alice = TestClient.register(port, "alice")) {
            TestClient.register(port, "bob").close(); // bob exists, then goes offline

            for (String id : List.of("m1", "m2", "m3")) {
                assertEquals("ACCEPTED", alice.sendMessage("bob", id, "body " + id).path("status").asText());
            }

            try (TestClient bob = TestClient.connect(port)) {
                bob.send(bob.registerFrame("bob"));
                assertEquals(3, bob.expect("REGISTERED").path("pending").asInt());
                for (String id : List.of("m1", "m2", "m3")) {
                    assertEquals(id, bob.expect("DELIVER").path("id").asText());
                }
            }
        }
    }

    // Scenario 7 / at-least-once: delivered but unacked survives a disconnect.
    @Test
    void unackedMessageIsRedeliveredAfterReconnect() throws IOException {
        startServer();
        try (TestClient alice = TestClient.register(port, "alice")) {
            try (TestClient bob = TestClient.register(port, "bob")) {
                alice.sendMessage("bob", "m1", "important");
                bob.expect("DELIVER"); // received, deliberately not acked
            }
            try (TestClient bob = TestClient.register(port, "bob")) {
                JsonNode again = bob.expect("DELIVER");
                assertEquals("m1", again.path("id").asText());
                assertEquals("OK", bob.ack("alice", "m1"));
            }
            try (TestClient bob = TestClient.register(port, "bob")) {
                // Acked -> gone. Prove nothing else is queued by checking the next frame is new.
                alice.sendMessage("bob", "m2", "next");
                assertEquals("m2", bob.expect("DELIVER").path("id").asText());
            }
        }
    }

    // A retry that arrives after the recipient already acked the first copy (the sender
    // lost its SEND_RESULT) must not be delivered again.
    @Test
    void retryAfterAckIsStillRecognisedAsDuplicate() throws IOException {
        startServer();
        try (TestClient alice = TestClient.register(port, "alice");
             TestClient bob = TestClient.register(port, "bob")) {
            alice.sendMessage("bob", "m1", "x");
            assertEquals("m1", bob.expect("DELIVER").path("id").asText());
            assertEquals("OK", bob.ack("alice", "m1"));

            JsonNode retry = alice.sendMessage("bob", "m1", "x");
            assertEquals("ACCEPTED", retry.path("status").asText());
            assertTrue(retry.path("duplicate").asBoolean());

            alice.sendMessage("bob", "m2", "y");
            assertEquals("m2", bob.expect("DELIVER").path("id").asText(), "m1 must not be redelivered");
        }
    }

    // Only the holder of the resume token issued at first REGISTER can reclaim a name.
    @Test
    void reRegisteringRequiresTheResumeToken() throws IOException {
        startServer();
        String token;
        try (TestClient bob = TestClient.connect(port)) {
            bob.send(frame("REGISTER").put("name", "bob"));
            token = bob.expect("REGISTERED").path("token").asText();
            assertTrue(token.length() >= 20, "token should carry ~128 bits: " + token);

            try (TestClient intruder = TestClient.connect(port)) {
                intruder.send(frame("REGISTER").put("name", "bob"));
                intruder.expectError("INVALID_TOKEN");
                intruder.send(frame("REGISTER").put("name", "bob").put("token", "guess"));
                intruder.expectError("INVALID_TOKEN");
                intruder.send(frame("PING"));
                intruder.expect("PONG"); // rejected but still connected; bob untouched
            }
            bob.send(frame("PING"));
            bob.expect("PONG");
        }
        try (TestClient bob = TestClient.connect(port)) {
            bob.send(frame("REGISTER").put("name", "bob").put("token", token));
            assertEquals(token, bob.expect("REGISTERED").path("token").asText());
        }
    }

    @Test
    void duplicateSendIsAcceptedButStoredOnce() throws IOException {
        startServer();
        try (TestClient alice = TestClient.register(port, "alice");
             TestClient bob = TestClient.register(port, "bob")) {
            assertFalse(alice.sendMessage("bob", "m1", "x").path("duplicate").asBoolean());
            JsonNode retry = alice.sendMessage("bob", "m1", "x");
            assertEquals("ACCEPTED", retry.path("status").asText());
            assertTrue(retry.path("duplicate").asBoolean());

            assertEquals("m1", bob.expect("DELIVER").path("id").asText());
            bob.ack("alice", "m1");
            alice.sendMessage("bob", "m2", "y");
            // If the duplicate had been stored, m1 would arrive again before m2.
            assertEquals("m2", bob.expect("DELIVER").path("id").asText());
        }
    }

    @Test
    void onlyTheRecipientCanAcknowledge() throws IOException {
        startServer();
        try (TestClient alice = TestClient.register(port, "alice");
             TestClient bob = TestClient.register(port, "bob");
             TestClient carol = TestClient.register(port, "carol")) {
            alice.sendMessage("bob", "m1", "for bob");
            bob.expect("DELIVER");
            assertEquals("STALE", carol.ack("alice", "m1"), "carol cannot remove bob's message");
            assertEquals("OK", bob.ack("alice", "m1"));
        }
    }

    @Test
    void resourceLimitsAreReportedPerSend() throws IOException {
        startServer("--mailbox-capacity=2", "--max-body-bytes=10", "--max-frame-bytes=1024");
        try (TestClient alice = TestClient.register(port, "alice")) {
            TestClient.register(port, "bob").close();

            assertEquals("ACCEPTED", alice.sendMessage("bob", "m1", "a").path("status").asText());
            assertEquals("ACCEPTED", alice.sendMessage("bob", "m2", "b").path("status").asText());
            assertRejected(alice.sendMessage("bob", "m3", "c"), "MAILBOX_FULL");
            assertTrue(alice.sendMessage("bob", "m1", "a").path("duplicate").asBoolean(),
                    "a retry of a stored message is a duplicate, not FULL");

            assertRejected(alice.sendMessage("bob", "big", "12345678901"), "MESSAGE_TOO_LARGE");
            assertRejected(alice.sendMessage("nobody", "m4", "a"), "UNKNOWN_RECIPIENT");
        }
    }

    // The server-wide byte budget is shared by all mailboxes and freed by acks.
    @Test
    void storageBudgetIsSharedAcrossMailboxesAndFreedByAck() throws IOException {
        startServer("--max-stored-bytes=10", "--max-body-bytes=8", "--max-frame-bytes=1024");
        try (TestClient alice = TestClient.register(port, "alice");
             TestClient bob = TestClient.register(port, "bob")) {
            TestClient.register(port, "carol").close();

            assertEquals("ACCEPTED", alice.sendMessage("bob", "m1", "12345678").path("status").asText());
            assertRejected(alice.sendMessage("carol", "m2", "abc"), "STORAGE_FULL");

            bob.expect("DELIVER");
            assertEquals("OK", bob.ack("alice", "m1"));
            assertEquals("ACCEPTED", alice.sendMessage("carol", "m2", "abc").path("status").asText());
        }
    }

    @Test
    void malformedInputIsReportedAndConnectionStaysUsable() throws IOException {
        startServer("--max-body-bytes=512", "--max-frame-bytes=1024");
        try (TestClient c = TestClient.connect(port)) {
            c.sendRaw("this is not json");
            c.expectError("BAD_REQUEST");
            c.sendRaw("{\"no\":\"type\"}");
            c.expectError("BAD_REQUEST");
            c.send(frame("SEND").put("to", "x").put("id", "1").put("body", "b"));
            c.expectError("NOT_REGISTERED");
            c.send(frame("REGISTER").put("name", "bad name!"));
            c.expectError("INVALID_NAME");
            c.send(frame("REGISTER").put("name", "alice"));
            c.expect("REGISTERED");
            c.send(frame("SEND").put("to", "alice").put("body", "no id"));
            c.expectError("BAD_REQUEST");
        }
        try (TestClient c = TestClient.connect(port)) {
            c.sendRaw("x".repeat(2000));
            c.expectError("FRAME_TOO_LARGE");
            c.expectClosed();
        }
    }

    @Test
    void connectionLimitIsEnforced() throws IOException {
        startServer("--max-connections=2");
        try (TestClient a = TestClient.register(port, "a");
             TestClient b = TestClient.register(port, "b");
             TestClient third = TestClient.connect(port)) {
            third.expectError("SERVER_BUSY");
            third.expectClosed();
        }
    }

    @Test
    void reRegisteringReplacesTheOldSession() throws IOException {
        startServer();
        try (TestClient alice = TestClient.register(port, "alice");
             TestClient oldBob = TestClient.register(port, "bob");
             TestClient newBob = TestClient.register(port, "bob")) {
            oldBob.expectError("SESSION_REPLACED");
            oldBob.expectClosed();
            alice.sendMessage("bob", "m1", "to the new session");
            assertEquals("m1", newBob.expect("DELIVER").path("id").asText());
        }
    }

    // A client that never reads must not delay anyone else.
    @Test
    void slowClientDoesNotBlockOthers() throws IOException {
        startServer("--max-body-bytes=60000", "--max-frame-bytes=131072", "--mailbox-capacity=500");
        String bigBody = "x".repeat(60_000);
        try (TestClient stalled = TestClient.connect(port, 1024);
             TestClient alice = TestClient.register(port, "alice");
             TestClient bob = TestClient.register(port, "bob");
             TestClient carol = TestClient.register(port, "carol")) {
            stalled.send(stalled.registerFrame("stalled")); // registers, then never reads

            // ~12 MB to a reader with a tiny receive buffer: its writer thread is now blocked
            // in socket write. Sends to it must still be answered immediately.
            for (int i = 0; i < 200; i++) {
                assertEquals("ACCEPTED", alice.sendMessage("stalled", "big" + i, bigBody).path("status").asText());
            }
            // Unrelated traffic still flows within the normal timeout.
            bob.sendMessage("carol", "m1", "unaffected");
            assertEquals("m1", carol.expect("DELIVER").path("id").asText());
            assertEquals("OK", carol.ack("bob", "m1"));
        }
    }

    // Concurrent senders: every message delivered exactly once in-session, per-sender order kept.
    @Test
    void concurrentSendersAreAllDeliveredInPerSenderOrder() throws Exception {
        startServer();
        int senders = 8;
        int perSender = 50;
        try (TestClient sink = TestClient.register(port, "sink");
             ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();
            for (int s = 0; s < senders; s++) {
                String name = "sender" + s;
                futures.add(pool.submit(() -> {
                    try (TestClient c = TestClient.register(port, name)) {
                        for (int i = 0; i < perSender; i++) {
                            c.send(frame("SEND").put("to", "sink").put("id", String.valueOf(i)).put("body", "b"));
                        }
                        for (int i = 0; i < perSender; i++) {
                            assertEquals("ACCEPTED", c.expect("SEND_RESULT").path("status").asText());
                        }
                    }
                    return null;
                }));
            }

            Map<String, Integer> lastSeen = new HashMap<>();
            int delivered = 0;
            int ackResults = 0;
            while (delivered < senders * perSender || ackResults < senders * perSender) {
                JsonNode f = sink.next();
                switch (f.path("type").asText()) {
                    case "DELIVER" -> {
                        String from = f.path("from").asText();
                        int id = f.path("id").asInt();
                        int previous = lastSeen.getOrDefault(from, -1);
                        assertEquals(previous + 1, id, "per-sender FIFO for " + from);
                        lastSeen.put(from, id);
                        delivered++;
                        sink.send(frame("ACK").put("from", from).put("id", f.path("id").asText()));
                    }
                    case "ACK_RESULT" -> {
                        assertEquals("OK", f.path("status").asText());
                        ackResults++;
                    }
                    default -> throw new AssertionError("unexpected " + f);
                }
            }
            for (Future<?> future : futures) {
                future.get();
            }
            assertEquals(senders, lastSeen.size());
        }
    }

    @Test
    void idleConnectionsAreClosed() throws IOException {
        startServer("--idle-timeout-ms=300");
        try (TestClient c = TestClient.register(port, "sleepy")) {
            c.expectError("IDLE_TIMEOUT");
            c.expectClosed();
        }
    }

    // A client that never registers is closed, even if it keeps the socket busy with PINGs.
    @Test
    void connectionsThatNeverRegisterAreClosed() throws Exception {
        startServer("--register-timeout-ms=300");
        try (TestClient silent = TestClient.connect(port)) {
            silent.expectError("REGISTER_TIMEOUT");
            silent.expectClosed();
        }
        try (TestClient pinger = TestClient.connect(port)) {
            // Pinging every 100 ms keeps the read timeout from firing, so only the
            // deadline check can end this. Bounded: TestClient reads time out at 2 s.
            JsonNode reply;
            do {
                Thread.sleep(100);
                pinger.send(frame("PING"));
                reply = pinger.next();
            } while (reply.path("type").asText().equals("PONG"));
            assertEquals("ERROR", reply.path("type").asText(), reply::toString);
            assertEquals("REGISTER_TIMEOUT", reply.path("code").asText(), reply::toString);
            pinger.expectClosed();
        }
    }

    @Test
    void shutdownNotifiesClientsAndCompletesPromptly() throws IOException {
        startServer();
        try (TestClient alice = TestClient.register(port, "alice")) {
            long start = System.nanoTime();
            server.close();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs < 3000, "shutdown took " + elapsedMs + " ms");
            alice.expect("SHUTDOWN");
            alice.expectClosed();
        }
    }

    private static void assertRejected(JsonNode result, String code) {
        assertEquals("REJECTED", result.path("status").asText(), result::toString);
        assertEquals(code, result.path("code").asText(), result::toString);
    }
}
