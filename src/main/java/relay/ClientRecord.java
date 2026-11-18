package relay;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;

/**
 * A logical client: its identity and mailbox, which outlive any single TCP connection,
 * plus the connection currently attached to it (or null while offline).
 *
 * <p>Concurrency: {@code this} is the one lock for both {@link #session} and the
 * {@link Mailbox}. Locks are per client, never global, so traffic to different
 * recipients never contends. No socket I/O happens while the lock is held. Waking a
 * connection's writer is a non-blocking semaphore release.
 *
 * <p>Identity: the first REGISTER of a name creates the record and receives a random
 * resume token. Every later REGISTER of that name must present it, so knowing a name
 * is no longer enough to take over its session and read its mailbox.
 */
final class ClientRecord {

    private static final SecureRandom RANDOM = new SecureRandom();

    private final String name;
    private final Mailbox mailbox;
    /** 128 random bits, base64url. Immutable, so it needs no lock. */
    private final String token;
    private Connection session; // guarded by this

    ClientRecord(String name, Mailbox mailbox) {
        this.name = name;
        this.mailbox = mailbox;
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);
        this.token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    String name() {
        return name;
    }

    String token() {
        return token;
    }

    /** Constant-time comparison, so response timing reveals nothing about the token. */
    boolean tokenMatches(String presented) {
        return presented != null && MessageDigest.isEqual(
                token.getBytes(StandardCharsets.US_ASCII), presented.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Attaches a connection, replacing any existing one ("new session wins"). In-flight
     * messages of the old session become pending again so they are redelivered on the
     * new one. Returns the replaced connection so the caller can close it outside the lock.
     */
    synchronized Connection attach(Connection connection) {
        Connection previous = session;
        session = connection;
        mailbox.resetInFlight();
        return previous == connection ? null : previous;
    }

    /** Detaches only if {@code connection} is still the current session (it may have been replaced). */
    synchronized void detach(Connection connection) {
        if (session == connection) {
            session = null;
            mailbox.resetInFlight();
        }
    }

    Mailbox.OfferResult offer(Message message) {
        Mailbox.OfferResult result;
        Connection current;
        synchronized (this) {
            result = mailbox.offer(message);
            current = session;
        }
        if (result == Mailbox.OfferResult.ACCEPTED && current != null) {
            current.wakeWriter();
        }
        return result;
    }

    /** Only the current session may pull messages; a replaced connection gets nothing. */
    synchronized List<Message> takeDeliverable(Connection connection, int window) {
        if (session != connection) {
            return List.of();
        }
        return mailbox.takeDeliverable(window);
    }

    /**
     * Acks are only honoured from the mailbox owner's current session. Because this
     * operates on the owner's own mailbox, another client can never remove it.
     */
    boolean ack(Connection connection, String from, String id) {
        boolean removed;
        synchronized (this) {
            if (session != connection) {
                return false;
            }
            removed = mailbox.ack(from, id);
        }
        if (removed) {
            connection.wakeWriter(); // a slot in the in-flight window was freed
        }
        return removed;
    }

    synchronized int pending() {
        return mailbox.size();
    }
}
