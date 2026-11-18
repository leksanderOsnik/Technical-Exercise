package relay;

import relay.Protocol.ErrorCode;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Accepts TCP connections and owns server lifecycle.
 *
 * <p>{@link #start()} binds and returns the actual port (use port 0 in tests to get a free
 * one). {@link #close()} stops accepting, sends SHUTDOWN to every client, waits up to
 * shutdown-timeout for connections to finish, then force-closes the rest.
 */
public final class RelayServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(RelayServer.class.getName());
    private static final long ACCEPT_BACKOFF_MS = 100;

    private final Config config;
    private final Registry registry;
    private final Semaphore connectionSlots;
    private final Set<Connection> connections = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final CountDownLatch terminated = new CountDownLatch(1);

    private ServerSocket serverSocket;
    private Thread acceptThread;

    public RelayServer(Config config) {
        this.config = config;
        this.registry = new Registry(config.maxClients(), config.mailboxCapacity(),
                new StorageBudget(config.maxStoredBytes()), config.dedupeWindow());
        this.connectionSlots = new Semaphore(config.maxConnections());
    }

    public synchronized int start() throws IOException {
        if (serverSocket != null) {
            throw new IllegalStateException("already started");
        }
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(config.host(), config.port()), 128);
        serverSocket = ss;
        acceptThread = Thread.ofPlatform().name("relay-accept").start(this::acceptLoop);
        return ss.getLocalPort();
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (!stopping.get()) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (!stopping.get()) {
                    // e.g. out of file descriptors: back off instead of spinning on the error.
                    LOG.log(System.Logger.Level.WARNING, "accept failed: {0}", e.getMessage());
                    try {
                        Thread.sleep(ACCEPT_BACKOFF_MS);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
                continue;
            }
            if (!connectionSlots.tryAcquire()) {
                rejectBusy(socket);
                continue;
            }
            try {
                socket.setTcpNoDelay(true);
            } catch (IOException ignored) {
                // not fatal
            }
            Connection connection = new Connection(socket, config, registry, c -> {
                connections.remove(c);
                connectionSlots.release();
            });
            connections.add(connection);
            connection.start();
        }
    }

    /** Over the connection limit: tell the client why, then close. Done off the accept thread. */
    private static void rejectBusy(Socket socket) {
        Thread.ofVirtual().start(() -> {
            try (socket) {
                OutputStream out = socket.getOutputStream();
                out.write((Protocol.error(ErrorCode.SERVER_BUSY, "connection limit reached") + "\n")
                        .getBytes(StandardCharsets.UTF_8));
                out.flush();
            } catch (IOException ignored) {
                // client is gone; nothing to do
            }
        });
    }

    @Override
    public void close() {
        if (!stopping.compareAndSet(false, true)) {
            awaitQuietly();
            return;
        }
        LOG.log(System.Logger.Level.INFO, "shutting down: {0} open connections", connections.size());
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            if (acceptThread != null) {
                acceptThread.join(TimeUnit.SECONDS.toMillis(1));
            }
            // Accept thread is gone, so the set can no longer grow.
            List<Connection> open = List.copyOf(connections);
            open.forEach(c -> c.closeGracefully(Protocol.shutdown()));
            long deadline = System.nanoTime() + config.shutdownTimeout().toNanos();
            for (Connection c : open) {
                long remaining = Math.max(0, deadline - System.nanoTime());
                if (!c.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                    c.close();
                }
            }
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "error during shutdown: {0}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            connections.forEach(Connection::close);
        } finally {
            terminated.countDown();
            LOG.log(System.Logger.Level.INFO, "shutdown complete");
        }
    }

    public void awaitTermination() throws InterruptedException {
        terminated.await();
    }

    private void awaitQuietly() {
        try {
            terminated.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
