package relay;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Name -> logical client. Records are never removed (in-memory, bounded by max-clients),
 * so an offline client's mailbox is retained until it reconnects or the server stops.
 */
final class Registry {

    private final ConcurrentHashMap<String, ClientRecord> clients = new ConcurrentHashMap<>();
    private final int maxClients;
    private final int mailboxCapacity;
    private final StorageBudget budget;
    private final int dedupeWindow;

    /** Result of {@link #getOrCreate}: {@code created} is true only for the caller that made the record. */
    record Registration(ClientRecord record, boolean created) {
    }

    Registry(int maxClients, int mailboxCapacity, StorageBudget budget, int dedupeWindow) {
        this.maxClients = maxClients;
        this.mailboxCapacity = mailboxCapacity;
        this.budget = budget;
        this.dedupeWindow = dedupeWindow;
    }

    /** Lock-free lookup on the hot path (every SEND). */
    ClientRecord get(String name) {
        return clients.get(name);
    }

    /**
     * Returns the existing record or creates one; null if the identity limit is reached.
     * Synchronized so the size check and insert are atomic, and so exactly one caller
     * sees {@code created == true} (and therefore receives the new resume token).
     * Registration is rare, so this lock is not on the message path.
     */
    synchronized Registration getOrCreate(String name) {
        ClientRecord existing = clients.get(name);
        if (existing != null) {
            return new Registration(existing, false);
        }
        if (clients.size() >= maxClients) {
            return null;
        }
        ClientRecord created = new ClientRecord(name, new Mailbox(mailboxCapacity, budget, dedupeWindow));
        clients.put(name, created);
        return new Registration(created, true);
    }

    int size() {
        return clients.size();
    }
}
