package relay;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * Per-recipient message store and delivery state machine. Pure logic, no I/O.
 *
 * <p><b>Not thread-safe.</b> Every call is made while holding the owning
 * {@link ClientRecord}'s monitor; that is the single lock guarding this state.
 *
 * <pre>
 *   offer() ──► PENDING ──takeDeliverable()──► IN_FLIGHT ──ack()──► removed
 *                  ▲                               │
 *                  └──────── resetInFlight() ◄─────┘   (disconnect / session replaced)
 * </pre>
 *
 * <p>Entries are kept in insertion order (LinkedHashMap) and always delivered from the
 * head, so delivery is FIFO per recipient, including after a reset/redelivery.
 *
 * <p>Dedupe covers stored messages and, through a bounded ring of fingerprints, the
 * last {@code dedupeWindow} acknowledged ones. Without the ring a sender that retries
 * after losing its SEND_RESULT would get its message delivered a second time if the
 * recipient had already acked the first copy.
 */
final class Mailbox {

    enum OfferResult { ACCEPTED, DUPLICATE, FULL, STORAGE_FULL }

    private enum State { PENDING, IN_FLIGHT }

    record Key(String from, String id) {
    }

    private static final class Entry {
        final Message message;
        final long bytes;
        State state = State.PENDING;

        Entry(Message message, long bytes) {
            this.message = message;
            this.bytes = bytes;
        }
    }

    private final int capacity;
    private final StorageBudget budget;
    private final LinkedHashMap<Key, Entry> entries = new LinkedHashMap<>();
    private int inFlight;

    /** Fingerprints of recently acked messages, a ring buffer. Allocated on first ack. */
    private final int dedupeWindow;
    private long[] acked;
    private int ackedNext;
    private int ackedCount;

    Mailbox(int capacity) {
        this(capacity, StorageBudget.unlimited(), 0);
    }

    Mailbox(int capacity, StorageBudget budget) {
        this(capacity, budget, 0);
    }

    Mailbox(int capacity, StorageBudget budget, int dedupeWindow) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (dedupeWindow < 0) {
            throw new IllegalArgumentException("dedupeWindow must be >= 0");
        }
        this.capacity = capacity;
        this.budget = Objects.requireNonNull(budget);
        this.dedupeWindow = dedupeWindow;
    }

    /**
     * Stores a message. A message whose (from, id) is already stored, or was among the
     * last {@code dedupeWindow} acknowledged, is a duplicate: it is reported to the
     * sender as accepted but not stored or delivered again (first copy wins).
     * Capacity is checked after the duplicate check so a retry of an already-stored
     * message is never reported as FULL. The shared byte budget is checked last, and
     * the reservation is released when the message is acknowledged.
     */
    OfferResult offer(Message message) {
        Objects.requireNonNull(message);
        Key key = new Key(message.from(), message.id());
        if (entries.containsKey(key) || (ackedCount > 0 && recentlyAcked(fingerprint(key)))) {
            return OfferResult.DUPLICATE;
        }
        if (entries.size() >= capacity) {
            return OfferResult.FULL;
        }
        long bytes = message.body().getBytes(StandardCharsets.UTF_8).length;
        if (!budget.tryReserve(bytes)) {
            return OfferResult.STORAGE_FULL;
        }
        entries.put(key, new Entry(message, bytes));
        return OfferResult.ACCEPTED;
    }

    /**
     * Marks up to {@code window - inFlight} PENDING messages (oldest first) as IN_FLIGHT
     * and returns them for writing to the socket. Returns an empty list when the
     * in-flight window is full or nothing is pending.
     *
     * <p>O(n) scan over a mailbox bounded by {@code capacity}; acceptable at these sizes.
     */
    List<Message> takeDeliverable(int window) {
        int room = window - inFlight;
        if (room <= 0) {
            return List.of();
        }
        List<Message> batch = new ArrayList<>(Math.min(room, entries.size()));
        for (Entry e : entries.values()) {
            if (batch.size() == room) {
                break;
            }
            if (e.state == State.PENDING) {
                e.state = State.IN_FLIGHT;
                batch.add(e.message);
            }
        }
        inFlight += batch.size();
        return batch;
    }

    /**
     * Removes the message if present. Returns false for unknown, already-acknowledged or
     * stale ids; that is harmless under at-least-once delivery, where a client may
     * legitimately ack the same message twice (e.g. after a redelivery).
     *
     * <p>A PENDING entry may also be acked: a client can receive a message, lose its
     * connection before acking, and send the ack first thing on the new connection.
     */
    boolean ack(String from, String id) {
        Key key = new Key(from, id);
        Entry removed = entries.remove(key);
        if (removed == null) {
            return false;
        }
        budget.release(removed.bytes);
        if (dedupeWindow > 0) {
            rememberAcked(fingerprint(key));
        }
        if (removed.state == State.IN_FLIGHT) {
            inFlight--;
        }
        return true;
    }

    /** Connection lost: everything delivered but unacknowledged becomes deliverable again. */
    void resetInFlight() {
        if (inFlight == 0) {
            return;
        }
        for (Entry e : entries.values()) {
            e.state = State.PENDING;
        }
        inFlight = 0;
    }

    int size() {
        return entries.size();
    }

    private boolean recentlyAcked(long fp) {
        // Linear scan over at most dedupeWindow longs: sub-microsecond at the default size,
        // and no boxing, so the window costs 8 bytes per remembered message.
        for (int i = 0; i < ackedCount; i++) {
            if (acked[i] == fp) {
                return true;
            }
        }
        return false;
    }

    private void rememberAcked(long fp) {
        if (acked == null) {
            acked = new long[dedupeWindow];
        }
        acked[ackedNext] = fp;
        ackedNext = (ackedNext + 1) % dedupeWindow;
        ackedCount = Math.min(ackedCount + 1, dedupeWindow);
    }

    /**
     * 64 bits of SHA-256 over (from, id). Sender names cannot contain '\n', so the
     * encoding is unambiguous. A cryptographic hash means a sender cannot craft an id
     * that collides with another sender's message; accidental collisions are about
     * window / 2^64 per send.
     */
    static long fingerprint(Key key) {
        MessageDigest sha;
        try {
            sha = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every Java platform", e);
        }
        byte[] d = sha.digest((key.from() + "\n" + key.id()).getBytes(StandardCharsets.UTF_8));
        long fp = 0;
        for (int i = 0; i < 8; i++) {
            fp = (fp << 8) | (d[i] & 0xFF);
        }
        return fp;
    }

    int inFlight() {
        return inFlight;
    }
}
