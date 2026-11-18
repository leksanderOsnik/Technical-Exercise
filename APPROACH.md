# Approach

## 1. Acceptance criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | Multiple clients can register at once with unique names | `registerSendDeliverAck`, `concurrentSenders…` |
| 2 | Either client can send a uniquely identified message to the other | `registerSendDeliverAck` |
| 3 | Every SEND gets `ACCEPTED` or `REJECTED` with a reason code | `registerSendDeliverAck`, `resourceLimitsAreReportedPerSend` |
| 4 | Recipient receives the message and must explicitly ACK it | `registerSendDeliverAck` |
| 5 | Messages to an offline recipient are retained, within limits | `offlineMessagesAreDeliveredInOrderAfterReconnect`, `resourceLimits…` |
| 6 | Re-registering the same name reattaches the same mailbox | `offlineMessages…`, `reRegisteringReplacesTheOldSession` |
| 7 | Delivered-but-unacked messages are redelivered after reconnect | `unackedMessageIsRedeliveredAfterReconnect`, `MailboxTest` |
| 8 | Only the recipient's ack removes a message; repeated acks are harmless | `onlyTheRecipientCanAcknowledge`, `MailboxTest.repeatedOrUnknownAckIsStale` |
| 9 | Duplicate sends are stored once, and a retry after the recipient acked is not delivered again | `duplicateSendIsAcceptedButStoredOnce`, `retryAfterAckIsStillRecognisedAsDuplicate`, `MailboxTest` |
| 10 | Mailboxes, total stored bytes, bodies, frames, connections and buffers are bounded; violations are reported | `resourceLimits…`, `storageBudget…`, `malformedInput…`, `connectionLimitIsEnforced`, `MailboxTest` |
| 11 | Malformed input is reported without killing the connection (except unrecoverable framing) | `malformedInputIsReportedAndConnectionStaysUsable` |
| 12 | A slow client does not block unrelated clients | `slowClientDoesNotBlockOthers` |
| 13 | Concurrent sends preserve correct state, with per-recipient FIFO (bonus 1) | `concurrentSendersAreAllDeliveredInPerSenderOrder`, `MailboxTest` |
| 14 | Idle connections are reaped; connections that never register are closed, even if they PING | `idleConnectionsAreClosed`, `connectionsThatNeverRegisterAreClosed` |
| 15 | Shutdown is prompt and announced | `shutdownNotifiesClientsAndCompletesPromptly` |
| 16 | Only the holder of a name's resume token can re-register it | `reRegisteringRequiresTheResumeToken` |

## 2. Architecture

```
            ┌──────────────── RelayServer ────────────────┐
 TCP ──────►│ accept thread ── Semaphore(max-connections) │
            │        │                                    │
            │        ▼  one per socket                    │
            │   Connection                                │
            │   ├─ reader (virtual thread) ──┐ parse, validate, mutate state
            │   │                            ▼
            │   │                   Registry (ConcurrentHashMap name → ClientRecord)
            │   │                            │
            │   │                   ClientRecord  [one lock per client]
            │   │                   ├─ Mailbox (ordered, bounded, PENDING/IN_FLIGHT)
            │   │                   └─ session → current Connection | null
            │   │                            │ wakeWriter() (non-blocking)
            │   └─ writer (virtual thread) ◄─┘ drains control queue, then pulls
            │        DELIVERs from own mailbox ───► socket
            └─────────────────────────────────────────────┘
```

The key separation is **logical client vs connection**. A `ClientRecord` (name and
mailbox) lives in the registry for the server's lifetime. A `Connection` is just the
current pipe to it. Disconnecting detaches the pipe; re-registering attaches a new one
to the same record. That is all reconnect is.

**Language and runtime:** Java 21 with plain `java.net` blocking sockets on **virtual
threads**. Blocking code (one reader and one writer per connection) is the simplest
code to reason about. Virtual threads make it cheap at thousands of connections
without moving to NIO selectors or a framework. The only runtime dependency is Jackson
for JSON.

## 3. Protocol

**Transport and framing:** TCP, one long-lived connection per client.
Newline-delimited JSON: one UTF-8 JSON object per line, CRLF tolerated. JSON escapes
newlines inside strings, so a raw `\n` always ends a frame. Lines over
`max-frame-bytes` are refused while being read, so the server never buffers an
unbounded line.

*Why not HTTP:* delivery is server push. HTTP would need long-polling or WebSockets,
and "connected" would stop being a real state. *Why not length-prefixed binary:*
newline JSON can be driven by hand with `nc`, which helps debugging and demos.
Length-prefix framing would be the choice for binary payloads or higher throughput.

### Frames

Client → server:

| type | fields | notes |
|---|---|---|
| `REGISTER` | `name`, `token` | Must be first. `name` matches `[A-Za-z0-9_-]{1,64}`. `token` is required when the name already exists (see below). |
| `SEND` | `id`, `to`, `body` | `id`: 1–128 chars, chosen by the sender, unique per sender (UUID recommended). |
| `ACK` | `from`, `id` | Acknowledges a delivered message. |
| `PING` | — | Keep-alive; answered with `PONG`. Allowed before REGISTER, but does not extend the register deadline. |

Server → client:

| type | fields | notes |
|---|---|---|
| `REGISTERED` | `name`, `pending`, `token` | `pending` = messages waiting in the mailbox. `token` = the name's resume token; store it. |
| `SEND_RESULT` | `id`, `status` (`ACCEPTED`/`REJECTED`), `duplicate`, `code`, `message` | One per SEND that has a valid `id`. |
| `DELIVER` | `from`, `id`, `body` | Pushed to the recipient. |
| `ACK_RESULT` | `from`, `id`, `status` (`OK`/`STALE`) | One per ACK. |
| `PONG` | — | |
| `ERROR` | `code`, `message` | For frames that cannot be answered with a typed result. |
| `SHUTDOWN` | — | Server is stopping; connection will close. |

Replies on one connection come back in request order (single reader, single writer),
so a client can correlate `SEND_RESULT`s by `id` or simply by order. `DELIVER` frames
are pushed asynchronously and may be interleaved between replies. For example, a send
to yourself can yield the `DELIVER` before its `SEND_RESULT`. Clients should dispatch
on `type`.

### Error codes

| code | sent as | connection |
|---|---|---|
| `BAD_REQUEST` | ERROR or SEND_RESULT | stays open |
| `NOT_REGISTERED` | ERROR | stays open |
| `INVALID_NAME` | ERROR | stays open |
| `INVALID_TOKEN` | ERROR | stays open; name exists and the token is missing or wrong |
| `TOO_MANY_CLIENTS` | ERROR | stays open |
| `UNKNOWN_RECIPIENT` | SEND_RESULT | stays open |
| `MAILBOX_FULL` | SEND_RESULT | stays open; the sender may retry later |
| `STORAGE_FULL` | SEND_RESULT | stays open; server-wide byte budget used up, retry later |
| `MESSAGE_TOO_LARGE` | SEND_RESULT | stays open |
| `FRAME_TOO_LARGE` | ERROR | **closed** (frame boundary lost, cannot resync) |
| `IDLE_TIMEOUT` | ERROR | **closed** |
| `REGISTER_TIMEOUT` | ERROR | **closed** (no REGISTER within `register-timeout-ms`) |
| `SESSION_REPLACED` | ERROR | **closed** (same name registered elsewhere) |
| `SERVER_BUSY` | ERROR | **closed** (connection limit) |

The rule: anything the server can still frame is answered and the connection
survives. Only framing loss, limits on the connection itself, and replacement close it.

### Connection lifecycle

1. Connect. If `max-connections` is reached: `SERVER_BUSY`, close. The connection must
   register within `register-timeout-ms` or it gets `REGISTER_TIMEOUT` and is closed, so
   unregistered sockets cannot hold connection slots.
2. `REGISTER {name, token?}`. A new name creates a mailbox and a random 128-bit
   **resume token**; any token sent with it is ignored. A known name reattaches to the
   existing mailbox only if `token` matches (constant-time compare); otherwise
   `INVALID_TOKEN`, and the connection stays open. If that name already has a live
   connection, **the new connection wins** and the old one gets `SESSION_REPLACED` and
   is closed. In-flight messages of the old session become pending again.
3. `REGISTERED {pending, token}`, then any queued messages are pushed as `DELIVER`s,
   oldest first.
4. Normal operation: SEND / ACK / PING in any order.
5. **Connection closes** (client quits, network drop, idle timeout, server shutdown):
   the session detaches; every delivered-but-unacked message returns to PENDING. The
   mailbox and identity remain. Nothing is lost.
6. **Reconnect** is just step 1–3 again with the same name and the stored token.

*Why "new connection wins" instead of rejecting a duplicate name:* after a network
failure, TCP often leaves a half-open connection the server hasn't noticed is dead
yet. Rejecting would lock an honest client out until the idle timeout fires. The
resume token is what makes this safe: only the client that created the name can
replace its session, so takeover needs the token, not just the name.

*Why a token issued by the server rather than a password:* there are no accounts to
manage, the client gets it for free on first registration, and 128 random bits can't
be guessed. It is a capability, not full authentication: it proves "I am whoever
registered this name first", which is exactly what reattaching a mailbox needs.

## 4. State model

```
Mailbox entry:   offer ─► PENDING ─deliver─► IN_FLIGHT ─ack─► (removed)
                             ▲                   │
                             └── disconnect ─────┘
```

- `Registry`: `ConcurrentHashMap<String, ClientRecord>`. Lookups on the send path are
  lock-free. Creation is synchronized so the `max-clients` check is atomic.
- `ClientRecord`: its monitor is **the** lock for its mailbox and its `session` pointer.
- `Mailbox`: `LinkedHashMap<(from,id), Entry>`. Insertion order gives FIFO. The key
  gives O(1) dedupe and ack. Bounded by `mailbox-capacity`, counting pending and
  in-flight messages. Next to it, a ring buffer of 64-bit fingerprints (first 8 bytes
  of SHA-256 over `from` and `id`) of the last `dedupe-window` acked messages: 8 bytes
  each, allocated on the first ack, scanned linearly. SHA-256 means a sender can't
  craft an id that collides with someone else's message.
- `StorageBudget`: one `AtomicLong` shared by all mailboxes. A message's body bytes are
  reserved (CAS loop, never over-commits) when it is accepted and released when it is
  acked. Being lock-free, it is used under a `ClientRecord` lock without adding a second
  lock, so the "one lock at a time" rule still holds.
- Message identity is `(sender, id)`, so ids only need to be unique per sender.

## 5. Concurrency model

- **One lock per client, never a global lock.** Sends to different recipients never
  contend. At most one `ClientRecord` lock is held at a time, so there is no lock
  ordering and no deadlock by construction.
- **No socket I/O while holding a lock.** A sender's reader thread appends to the
  recipient's mailbox under the recipient's lock, then calls `wakeWriter()` (a
  semaphore release) and returns. It never touches the recipient's socket.
- **Single writer per connection.** Only the connection's writer thread writes to its
  socket. It drains queued replies first, then pulls from its own mailbox up to the
  in-flight window. That keeps wire order consistent, for example `REGISTERED` before
  the first `DELIVER`.
- **Ownership check.** `takeDeliverable` and `ack` succeed only for the record's current
  session, so a replaced connection can neither pull nor remove messages.
- **Slow-client isolation:**
  - A client that doesn't read: only its own writer thread blocks in `write()`. Its
    mailbox fills, and senders get `MAILBOX_FULL` (backpressure, not blocking).
  - A client that keeps sending but never reads replies: its bounded reply queue fills
    and it is disconnected. Its messages stay in the mailbox.
  - A silent client: closed after `idle-timeout-ms` (or `register-timeout-ms` before it
    registers).
- **Final frames are never dropped.** `closeGracefully` queues the final frame and only
  then sets the volatile `draining` flag. When the writer sees the flag it drains the
  queue once more before closing, so `SHUTDOWN` / `SESSION_REPLACED` cannot be lost to a
  race with the writer's previous drain.
- **REGISTER racing with close.** The reader publishes `client` then checks `closed`;
  `close()` sets `closed` then reads `client`. Both are volatile, so at least one side
  sees the other and detaches. Without that, a record could stay attached to a dead
  connection until its next REGISTER.
- **Unexpected errors** in the reader are logged and close only that connection; its
  unacked messages return to PENDING as on any disconnect.
- **Shutdown:** close the listening socket → join the accept thread → send `SHUTDOWN` and
  drain each connection (up to 1 s flush) → wait up to `shutdown-timeout-ms` → force-close
  the rest. Bounded and predictable.

## 6. Delivery semantics

- **At-least-once.** A message is removed only when the recipient's own session ACKs it.
  If a connection drops between DELIVER and ACK, the message is delivered again on
  reconnect. Recipients should treat `(from, id)` as an idempotency key.
- **Ordering: FIFO per recipient**, in the order the server accepted the messages. That
  means per-sender order is preserved, while sends from different senders interleave in
  acceptance order. Redelivery after reconnect restarts from the head of the mailbox,
  so FIFO holds across reconnects. With `inflight-window > 1` a client may receive the
  next messages before acking earlier ones; set the window to 1 for strict
  one-at-a-time delivery.
- **Duplicate sends:** a SEND whose `(from, id)` is still in the mailbox, or is one of
  the recipient's last `dedupe-window` acked messages, returns `ACCEPTED` with
  `duplicate: true` and is not stored or delivered again. The first copy wins, even if
  the body differs. A retried send after a lost `SEND_RESULT` is therefore safe even
  when the recipient has already acked the original.
- **Repeated or stale ACKs:** an ACK for an unknown or already-removed message returns
  `STALE`. It is a harmless no-op, never an error. An ACK may also remove a message
  that is PENDING: a client that received a message, lost its connection, and acks
  right after reconnecting doesn't get it redelivered.
- **Wrong recipient:** an ACK only searches the acking client's own mailbox, so another
  client can never remove it.

## 7. Trade-offs

| Decision | Alternative | Why this one |
|---|---|---|
| TCP + newline JSON | HTTP / gRPC, length-prefixed binary | Push delivery and real connection state; debuggable with `nc` |
| Blocking I/O on virtual threads | NIO selector / Netty | Simplest correct code; scales well enough; the relay logic stays visible |
| New session replaces old, with resume token | Reject `NAME_IN_USE`; or no token | Handles half-open TCP; the token stops impersonation |
| Unknown recipient → reject | Create mailbox on first send | Prevents unbounded mailbox creation by typos or abuse |
| Mailbox full → reject send | Drop oldest, or block sender | Never silently lose an accepted message; never block a sender's connection |
| Disconnect slow readers | Unbounded buffering | Bounded memory; at-least-once makes the disconnect lossless |
| Global byte budget + per-mailbox count | Per-mailbox limits only | Count limits alone multiply to an unusable bound (10k × 1k × 16 KB ≈ 160 GB) |
| Dedupe while stored + last N acked (fingerprints) | Remember all ids forever; or only while stored | Covers realistic retries in bounded memory (8 bytes per remembered id) |

## 8. Known limitations

- **In memory only.** All mailboxes are lost on restart or crash. Bonus 3 (persistence)
  is not implemented.
- **Tokens are bearer secrets over plain TCP.** Anyone who can sniff the connection
  can read the token. TLS would fix that and is out of scope. A client that loses its
  token can't reclaim its name until the server restarts (the name then is new).
- **Dedupe is bounded.** A retry arriving after more than `dedupe-window` later acks to
  the same recipient is accepted as new. A 64-bit fingerprint collision would make a
  new message look like a duplicate; the odds are about `dedupe-window / 2^64` per send.
- **Identities never expire.** Mailboxes of clients that never return are kept until
  restart (bounded by `max-clients`). There is no message TTL.
- **The byte budget counts bodies only**, not ids, sender names or object overhead, so
  real heap use is somewhat higher than `max-stored-bytes`. Size the JVM heap with
  headroom.
- **No per-sender fairness or rate limit.** One sender can fill a recipient's mailbox,
  or the shared budget, by spreading messages across recipients.
- `takeDeliverable` scans the mailbox from the head: O(capacity) per call. That's fine
  at the default sizes, but a cursor would be needed for large mailboxes.
- Rejected over-limit connections each get a short-lived virtual thread to write
  `SERVER_BUSY`. Under a connection flood this is still unbounded thread creation
  (cheap, but not capped).
- The final "shutdown complete" log line can be lost on SIGTERM, because the JVM's
  logging shuts down in a parallel shutdown hook. Shutdown itself still completes.

## 9. Testing

`./mvnw test` runs 28 tests in about 2 s of test time:

- **`MailboxTest`** (unit): the state machine. Deterministic, no I/O.
- **`RelayIntegrationTest`** (end to end): real TCP against a server on port 0, one test
  per row in section 1.
- **`ConfigTest`**: option precedence and validation.

Determinism: no fixed ports, and no sleeps to wait for results. Each test waits for a
specific frame. Every socket read times out at 2 s, each test has `@Timeout(15)`, and
Surefire has a forked-JVM exit timeout. The suite passed 5 runs in a row locally.

**Not covered, deliberately:** load or soak testing, real network faults (packet loss,
true half-open sockets; takeover and idle timeout are tested as proxies), fuzzing of
the JSON parser, and the CLI demo client. The slow-client test checks the observable
property (others unaffected while one client is stalled). 12 MB is queued in the
stalled client's mailbox; the in-flight window lets about 2 MB of it reach the socket,
which is enough to fill a 1 KB receive buffer and block that client's writer. It does
not inspect thread states. The drain and register/close races are fixed by reasoning
about volatile ordering (documented above) rather than by a test, since a deterministic
test would need hooks into thread scheduling.

## 10. Next steps

1. **Persistence (bonus 3):** append-only log of `ACCEPT`/`ACK` records per mailbox, with
   fsync before replying `ACCEPTED` (or batched fsync as a latency trade-off). Replay
   on startup; compact periodically. If the disk is full or a write fails, reject sends
   with a new `STORAGE_UNAVAILABLE` code rather than accepting what can't be persisted.
2. Message TTL and idle-identity expiry.
3. TLS, so resume tokens and bodies are not readable on the wire; token rotation.
4. Metrics (connections, mailbox depth, rejects by code) and structured logs.
5. Pin Docker base images by digest; add a container smoke test in CI.

## 11. AI-tool usage

AI assistance was used in every step of the process: planning, design,
implementation, testing and documentation. The result was verified with the
automated test suite (`./mvnw test`) and CI.
