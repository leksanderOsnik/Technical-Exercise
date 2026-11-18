package relay;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Deterministic unit tests of the delivery state machine (no I/O, no threads). */
class MailboxTest {

    private static Message msg(String from, String id) {
        return new Message(from, id, "body-" + id);
    }

    private static List<String> ids(List<Message> messages) {
        return messages.stream().map(Message::id).toList();
    }

    @Test
    void deliversInFifoOrderWithinWindow() {
        Mailbox box = new Mailbox(10);
        for (String id : List.of("1", "2", "3", "4")) {
            box.offer(msg("a", id));
        }
        assertEquals(List.of("1", "2"), ids(box.takeDeliverable(2)));
        assertEquals(List.of(), ids(box.takeDeliverable(2)), "window full until an ack");
        assertTrue(box.ack("a", "1"));
        assertEquals(List.of("3"), ids(box.takeDeliverable(2)));
    }

    @Test
    void resetRedeliversUnackedInOriginalOrder() {
        Mailbox box = new Mailbox(10);
        box.offer(msg("a", "1"));
        box.offer(msg("a", "2"));
        box.offer(msg("a", "3"));
        box.takeDeliverable(10);
        box.ack("a", "2");

        box.resetInFlight(); // disconnect
        assertEquals(0, box.inFlight());
        assertEquals(List.of("1", "3"), ids(box.takeDeliverable(10)));
    }

    @Test
    void duplicateIsNotStoredTwiceAndIsNotReportedFull() {
        Mailbox box = new Mailbox(1);
        assertEquals(Mailbox.OfferResult.ACCEPTED, box.offer(msg("a", "1")));
        assertEquals(Mailbox.OfferResult.DUPLICATE, box.offer(msg("a", "1")));
        assertEquals(Mailbox.OfferResult.FULL, box.offer(msg("a", "2")));
        assertEquals(1, box.size());
    }

    @Test
    void sameIdFromDifferentSendersAreDistinct() {
        Mailbox box = new Mailbox(10);
        assertEquals(Mailbox.OfferResult.ACCEPTED, box.offer(msg("a", "1")));
        assertEquals(Mailbox.OfferResult.ACCEPTED, box.offer(msg("b", "1")));
        assertEquals(2, box.size());
    }

    @Test
    void repeatedOrUnknownAckIsStale() {
        Mailbox box = new Mailbox(10);
        box.offer(msg("a", "1"));
        box.takeDeliverable(10);
        assertTrue(box.ack("a", "1"));
        assertFalse(box.ack("a", "1"), "second ack");
        assertFalse(box.ack("a", "nope"), "unknown id");
        assertFalse(box.ack("b", "1"), "wrong sender");
        assertEquals(0, box.inFlight());
    }

    @Test
    void sharedStorageBudgetIsReservedOnOfferAndReleasedOnAck() {
        StorageBudget budget = new StorageBudget(15); // bodies are "body-N": 6 bytes each
        Mailbox bob = new Mailbox(10, budget);
        Mailbox carol = new Mailbox(10, budget);
        assertEquals(Mailbox.OfferResult.ACCEPTED, bob.offer(msg("a", "1")));
        assertEquals(Mailbox.OfferResult.ACCEPTED, carol.offer(msg("a", "2")));
        assertEquals(Mailbox.OfferResult.STORAGE_FULL, carol.offer(msg("a", "3")), "12 + 6 > 15");
        assertEquals(Mailbox.OfferResult.DUPLICATE, carol.offer(msg("a", "2")), "dup costs nothing");
        assertEquals(12L, budget.used());

        assertTrue(bob.ack("a", "1"));
        assertEquals(6L, budget.used());
        assertFalse(bob.ack("a", "1"), "a stale ack must not release twice");
        assertEquals(6L, budget.used());
        assertEquals(Mailbox.OfferResult.ACCEPTED, carol.offer(msg("a", "3")));
    }

    @Test
    void ackOfPendingMessageRemovesItWithoutCorruptingWindow() {
        Mailbox box = new Mailbox(10);
        box.offer(msg("a", "1"));
        box.offer(msg("a", "2"));
        assertTrue(box.ack("a", "1")); // e.g. ack sent right after reconnect, before redelivery
        assertEquals(0, box.inFlight());
        assertEquals(List.of("2"), ids(box.takeDeliverable(1)));
    }

    @Test
    void ackedMessagesAreRememberedWithinTheDedupeWindow() {
        Mailbox box = new Mailbox(10, StorageBudget.unlimited(), 2);
        for (String id : List.of("1", "2", "3")) {
            box.offer(msg("a", id));
            box.takeDeliverable(10);
            assertTrue(box.ack("a", id));
        }
        assertEquals(Mailbox.OfferResult.DUPLICATE, box.offer(msg("a", "3")));
        assertEquals(Mailbox.OfferResult.DUPLICATE, box.offer(msg("a", "2")));
        assertEquals(Mailbox.OfferResult.ACCEPTED, box.offer(msg("a", "1")), "evicted from a window of 2");
        assertEquals(Mailbox.OfferResult.ACCEPTED, box.offer(msg("b", "3")), "other sender, same id");
        assertFalse(box.ack("a", "3"), "remembered, not stored: ack stays STALE");
    }

    @Test
    void dedupeWindowOfZeroForgetsAckedMessages() {
        Mailbox box = new Mailbox(10, StorageBudget.unlimited(), 0);
        box.offer(msg("a", "1"));
        box.ack("a", "1");
        assertEquals(Mailbox.OfferResult.ACCEPTED, box.offer(msg("a", "1")));
    }
}
