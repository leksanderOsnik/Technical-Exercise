package relay;

/**
 * An accepted message. Identity is the pair (from, id): ids only need to be unique
 * per sender, so one client can never collide with another client's ids.
 */
record Message(String from, String id, String body) {
}
