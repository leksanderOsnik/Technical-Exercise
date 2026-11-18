package relay;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Server-wide cap on the bytes held in all mailboxes together. Per-mailbox counts alone
 * only bound memory at {@code max-clients × mailbox-capacity × max-body-bytes}, which is
 * far too large to be a real limit. This makes the total an explicit, enforced number.
 *
 * <p>Lock-free so it can be called while holding a {@link ClientRecord} lock without
 * adding a second lock (and so without any lock-ordering concerns).
 */
final class StorageBudget {

    private final long limit;
    private final AtomicLong used = new AtomicLong();

    StorageBudget(long limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }
        this.limit = limit;
    }

    static StorageBudget unlimited() {
        return new StorageBudget(Long.MAX_VALUE);
    }

    /** Reserves {@code bytes} if they fit under the limit; never over-commits. */
    boolean tryReserve(long bytes) {
        while (true) {
            long current = used.get();
            if (bytes > limit - current) {
                return false;
            }
            if (used.compareAndSet(current, current + bytes)) {
                return true;
            }
        }
    }

    void release(long bytes) {
        used.addAndGet(-bytes);
    }

    long used() {
        return used.get();
    }
}
