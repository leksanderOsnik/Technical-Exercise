package relay;

import com.fasterxml.jackson.databind.JsonNode;
import relay.Protocol.ErrorCode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * One TCP connection, served by two virtual threads:
 *
 * <ul>
 *   <li><b>reader</b>: parses frames and applies them to shared state. It never writes
 *       to the socket; replies go into a bounded {@link #control} queue.</li>
 *   <li><b>writer</b>: the only thread that touches the socket output. It drains control
 *       replies first, then pulls DELIVER frames straight from the client's mailbox
 *       (bounded by the in-flight window). Being a single writer is what keeps frames
 *       ordered on the wire.</li>
 * </ul>
 *
 * <p>Isolation: other clients only ever touch this connection through non-blocking
 * calls ({@link #wakeWriter()}). If this client stops reading, only its own writer
 * blocks. If it keeps sending while never reading, the control queue fills and the
 * connection is closed. Its messages stay safe in the mailbox.
 */
final class Connection {

    private static final System.Logger LOG = System.getLogger(Connection.class.getName());
    /** How long a closing connection may spend flushing its final frames. */
    private static final Duration FLUSH_GRACE = Duration.ofSeconds(1);

    private final Socket socket;
    private final Config config;
    private final Registry registry;
    private final Consumer<Connection> onClose;
    private final String remote;

    private final BlockingQueue<String> control;
    private final Semaphore wake = new Semaphore(0);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch readerDone = new CountDownLatch(1);
    private final CountDownLatch writerDone = new CountDownLatch(1);

    /** Set once by the reader after REGISTER; read by the writer. */
    private volatile ClientRecord client;
    /** When set, the writer flushes remaining control frames then closes the socket. */
    private volatile boolean draining;
    /** System.nanoTime() by which REGISTER must succeed. Only read by the reader. */
    private final long registerDeadline;

    Connection(Socket socket, Config config, Registry registry, Consumer<Connection> onClose) {
        this.socket = socket;
        this.config = config;
        this.registry = registry;
        this.onClose = onClose;
        this.remote = String.valueOf(socket.getRemoteSocketAddress());
        this.control = new ArrayBlockingQueue<>(config.outboundQueue());
        this.registerDeadline = System.nanoTime() + config.registerTimeout().toNanos();
    }

    void start() {
        Thread.ofVirtual().name("reader " + remote).start(this::readLoop);
        Thread.ofVirtual().name("writer " + remote).start(this::writeLoop);
    }

    /** Non-blocking; safe to call from any thread, including while holding a ClientRecord lock. */
    void wakeWriter() {
        wake.release();
    }

    // ---- reader ------------------------------------------------------------------

    private void readLoop() {
        try {
            // Until REGISTER succeeds the read timeout is the (shorter) register timeout;
            // onRegister switches it to the idle timeout.
            socket.setSoTimeout((int) config.registerTimeout().toMillis());
            FrameReader reader = new FrameReader(
                    new BufferedInputStream(socket.getInputStream()), config.maxFrameBytes());
            String line;
            while (!closed.get() && !draining && (line = reader.read()) != null) {
                if (!line.isBlank()) {
                    handle(line);
                }
            }
        } catch (FrameReader.FrameTooLargeException e) {
            closeGracefully(Protocol.error(ErrorCode.FRAME_TOO_LARGE, e.getMessage()));
        } catch (SocketTimeoutException e) {
            if (client == null) {
                closeGracefully(registerTimeout());
            } else {
                closeGracefully(Protocol.error(ErrorCode.IDLE_TIMEOUT, "no frame received within "
                        + config.idleTimeout().toMillis() + " ms"));
            }
        } catch (IOException e) {
            // peer went away or we closed the socket; nothing to report
        } catch (RuntimeException e) {
            // A bug, not a client error. Log it and drop only this connection; the
            // finally block detaches it so its unacked messages are redelivered.
            LOG.log(System.Logger.Level.ERROR, "unexpected error on " + remote + ", closing", e);
        } finally {
            if (!draining) {
                close();
            }
            readerDone.countDown();
        }
    }

    private void handle(String line) {
        JsonNode frame;
        try {
            frame = Protocol.parse(line);
        } catch (Protocol.ProtocolException e) {
            reply(Protocol.error(e.code, e.getMessage()));
            return;
        }
        String type = Protocol.text(frame, "type");
        if (type == null) {
            reply(Protocol.error(ErrorCode.BAD_REQUEST, "missing string field 'type'"));
            return;
        }
        if (client == null && System.nanoTime() - registerDeadline > 0) {
            // Covers a client that keeps the socket alive with PINGs but never registers,
            // which would otherwise hold a connection slot forever.
            closeGracefully(registerTimeout());
            return;
        }
        if (!type.equals("REGISTER") && !type.equals("PING") && client == null) {
            reply(Protocol.error(ErrorCode.NOT_REGISTERED, "REGISTER first"));
            return;
        }
        switch (type) {
            case "REGISTER" -> onRegister(frame);
            case "SEND" -> onSend(frame);
            case "ACK" -> onAck(frame);
            case "PING" -> reply(Protocol.pong());
            default -> reply(Protocol.error(ErrorCode.BAD_REQUEST, "unknown type '" + type + "'"));
        }
    }

    private void onRegister(JsonNode frame) {
        if (client != null) {
            reply(Protocol.error(ErrorCode.BAD_REQUEST, "already registered as '" + client.name() + "'"));
            return;
        }
        String name = Protocol.text(frame, "name");
        if (!Protocol.validName(name)) {
            reply(Protocol.error(ErrorCode.INVALID_NAME, "name must match [A-Za-z0-9_-]{1,64}"));
            return;
        }
        Registry.Registration registration = registry.getOrCreate(name);
        if (registration == null) {
            reply(Protocol.error(ErrorCode.TOO_MANY_CLIENTS, "identity limit reached"));
            return;
        }
        ClientRecord record = registration.record();
        // A new name needs no token (any token sent, e.g. one from before a server
        // restart, is ignored). An existing name needs the token issued when it was
        // created. The connection stays open so the client can retry.
        if (!registration.created() && !record.tokenMatches(Protocol.text(frame, "token"))) {
            reply(Protocol.error(ErrorCode.INVALID_TOKEN, "'" + name
                    + "' is already registered; send the token from its first REGISTERED reply"));
            return;
        }
        Connection previous = record.attach(this);
        if (previous != null) {
            LOG.log(System.Logger.Level.INFO, "{0} re-registered from {1}; replacing old session", name, remote);
            previous.closeGracefully(Protocol.error(ErrorCode.SESSION_REPLACED,
                    "name registered by another connection"));
        }
        // Queue REGISTERED before publishing `client`, so the writer cannot send a DELIVER
        // ahead of the REGISTERED reply.
        reply(Protocol.registered(name, record.pending(), record.token()));
        client = record;
        // close() sets `closed` and then reads `client`; we wrote `client` and now read
        // `closed`. Both are volatile, so at least one side sees the other. Without this
        // check a close racing with REGISTER would leave the record attached to a dead
        // connection. detach() is idempotent, so both sides detaching is harmless.
        if (closed.get()) {
            record.detach(this);
            return;
        }
        try {
            socket.setSoTimeout((int) config.idleTimeout().toMillis());
        } catch (SocketException e) {
            close();
            return;
        }
        wakeWriter();
        LOG.log(System.Logger.Level.DEBUG, "{0} registered from {1}", name, remote);
    }

    private void onSend(JsonNode frame) {
        String id = Protocol.text(frame, "id");
        if (!Protocol.validId(id)) {
            reply(Protocol.error(ErrorCode.BAD_REQUEST,
                    "SEND requires string 'id' of 1-" + Protocol.MAX_ID_LENGTH + " chars"));
            return;
        }
        String to = Protocol.text(frame, "to");
        String body = Protocol.text(frame, "body");
        if (to == null || body == null) {
            reply(Protocol.sendRejected(id, ErrorCode.BAD_REQUEST, "SEND requires string 'to' and 'body'"));
            return;
        }
        if (body.getBytes(StandardCharsets.UTF_8).length > config.maxBodyBytes()) {
            reply(Protocol.sendRejected(id, ErrorCode.MESSAGE_TOO_LARGE,
                    "body exceeds " + config.maxBodyBytes() + " bytes"));
            return;
        }
        ClientRecord recipient = registry.get(to);
        if (recipient == null) {
            reply(Protocol.sendRejected(id, ErrorCode.UNKNOWN_RECIPIENT, "'" + to + "' has never registered"));
            return;
        }
        switch (recipient.offer(new Message(client.name(), id, body))) {
            case ACCEPTED -> reply(Protocol.sendAccepted(id, false));
            case DUPLICATE -> reply(Protocol.sendAccepted(id, true));
            case FULL -> reply(Protocol.sendRejected(id, ErrorCode.MAILBOX_FULL,
                    "mailbox of '" + to + "' is full"));
            case STORAGE_FULL -> reply(Protocol.sendRejected(id, ErrorCode.STORAGE_FULL,
                    "server storage limit reached"));
        }
    }

    private void onAck(JsonNode frame) {
        String from = Protocol.text(frame, "from");
        String id = Protocol.text(frame, "id");
        if (from == null || id == null) {
            reply(Protocol.error(ErrorCode.BAD_REQUEST, "ACK requires string 'from' and 'id'"));
            return;
        }
        reply(Protocol.ackResult(from, id, client.ack(this, from, id)));
    }

    private String registerTimeout() {
        return Protocol.error(ErrorCode.REGISTER_TIMEOUT, "no REGISTER within "
                + config.registerTimeout().toMillis() + " ms");
    }

    /** Queue a reply for the writer. A client that never drains its replies is disconnected. */
    private void reply(String frame) {
        if (control.offer(frame)) {
            wakeWriter();
        } else {
            LOG.log(System.Logger.Level.WARNING, "{0}: outbound queue full (slow consumer), closing", remote);
            close();
        }
    }

    // ---- writer ------------------------------------------------------------------

    private void writeLoop() {
        try {
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            while (!closed.get()) {
                boolean wrote = false;
                String frame;
                while ((frame = control.poll()) != null) {
                    writeFrame(out, frame);
                    wrote = true;
                }
                if (draining) {
                    // closeGracefully() queues the final frame *before* setting `draining`
                    // (volatile), so once we see the flag the frame is visible: drain once
                    // more or it could be lost if it arrived after the loop above.
                    while ((frame = control.poll()) != null) {
                        writeFrame(out, frame);
                    }
                    out.flush();
                    break;
                }
                ClientRecord c = client;
                if (c != null) {
                    for (Message m : c.takeDeliverable(this, config.inflightWindow())) {
                        writeFrame(out, Protocol.deliver(m));
                        wrote = true;
                    }
                }
                if (wrote) {
                    out.flush();
                    continue; // re-check: more work may have arrived while writing
                }
                wake.acquire();      // a release() before this point leaves a permit, so no lost wake-up
                wake.drainPermits(); // coalesce multiple signals into one pass
            }
        } catch (IOException | InterruptedException e) {
            // socket closed or broken
        } finally {
            close();
            writerDone.countDown();
        }
    }

    private static void writeFrame(OutputStream out, String frame) throws IOException {
        out.write(frame.getBytes(StandardCharsets.UTF_8));
        out.write('\n');
    }

    // ---- lifecycle ---------------------------------------------------------------

    /**
     * Sends a final frame (best effort), lets the writer flush it, then closes. A forced
     * close follows after {@link #FLUSH_GRACE} in case the peer is not reading (for
     * example a half-open connection).
     */
    void closeGracefully(String finalFrame) {
        if (closed.get()) {
            return;
        }
        if (finalFrame != null) {
            control.offer(finalFrame); // best effort; if full, the client is not reading anyway
        }
        draining = true;
        wakeWriter();
        Thread.ofVirtual().name("closer " + remote).start(() -> {
            try {
                writerDone.await(FLUSH_GRACE.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            close();
        });
    }

    /** Idempotent hard close. Detaches from the client so unacked messages get redelivered. */
    void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            socket.close(); // unblocks the reader and a writer stuck in write()
        } catch (IOException ignored) {
            // nothing useful to do
        }
        ClientRecord c = client;
        if (c != null) {
            c.detach(this);
        }
        wakeWriter();
        onClose.accept(this);
    }

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        return readerDone.await(timeout, unit)
                && writerDone.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    }
}
