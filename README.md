# Message Relay

A small TCP client/server message relay with per-recipient mailboxes, explicit
acknowledgements, offline retention and at-least-once redelivery. Design and
trade-offs are in [APPROACH.md](APPROACH.md).

## Prerequisites

- **JDK 21** (any distribution). That is all: the Maven wrapper (`./mvnw`) downloads
  Maven 3.9.12 from Maven Central on first use.
- Optional: Docker, and `nc` for poking at the protocol by hand.

## Build, test, run

```bash
./mvnw test        # all automated tests (unit + real-socket integration), ~5 s after first build
./mvnw package     # tests + builds the artifact
java -jar target/relay.jar                 # start server on 0.0.0.0:7000
java -jar target/relay.jar --help          # list all options
```

**Artifact:** `target/relay.jar`, a self-contained runnable jar (Jackson bundled).
`target/original-relay.jar` is the unshaded intermediate and can be ignored.

### Docker

```bash
docker build -t message-relay .
docker run --rm -p 7000:7000 message-relay
docker run --rm -p 7000:7000 -e RELAY_MAILBOX_CAPACITY=50 message-relay   # override a limit
```

The image runs the jar as a non-root user on `eclipse-temurin:21-jre`. `docker stop`
sends SIGTERM, which triggers a graceful shutdown.

## Try it

Terminal 1: `java -jar target/relay.jar`

Terminal 2 (interactive client, auto-acks deliveries):

```bash
java -cp target/relay.jar relay.cli.RelayClient bob
```

Terminal 3:

```bash
java -cp target/relay.jar relay.cli.RelayClient alice
```

then type `send bob hello`. Quit bob (`quit`), send more from alice, start bob again:
the queued messages are delivered on reconnect.

Re-registering an existing name needs the **resume token** the server returned in the
first `REGISTERED` reply. The demo client saves it to `~/.relay-tokens/` and sends it
automatically; without it the server answers `INVALID_TOKEN`.

Or raw, with netcat (one JSON object per line):

```bash
nc localhost 7000
{"type":"REGISTER","name":"alice"}
{"type":"SEND","id":"m1","to":"alice","body":"note to self"}
{"type":"ACK","from":"alice","id":"m1"}
```

To reconnect as `alice` from another `nc`, copy the `token` from the `REGISTERED` reply:
`{"type":"REGISTER","name":"alice","token":"<token>"}`.

## Configuration

Each option can be set as a flag (`--name=value` or `--name value`) or an environment
variable. Precedence: flag > env var > default.

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `--host` | `RELAY_HOST` | `0.0.0.0` | Bind address |
| `--port` | `RELAY_PORT` | `7000` | Listen port (`0` = pick a free port) |
| `--max-connections` | `RELAY_MAX_CONNECTIONS` | `1000` | Concurrent TCP connections; extra ones get `SERVER_BUSY` and are closed |
| `--max-clients` | `RELAY_MAX_CLIENTS` | `10000` | Distinct registered names (each owns a mailbox) |
| `--mailbox-capacity` | `RELAY_MAILBOX_CAPACITY` | `1000` | Unacked messages per recipient; beyond this, sends get `MAILBOX_FULL` |
| `--max-stored-bytes` | `RELAY_MAX_STORED_BYTES` | `268435456` | Total body bytes held across **all** mailboxes (256 MiB); beyond this, sends get `STORAGE_FULL` |
| `--max-body-bytes` | `RELAY_MAX_BODY_BYTES` | `16384` | Max UTF-8 size of a message body |
| `--max-frame-bytes` | `RELAY_MAX_FRAME_BYTES` | `65536` | Max raw line length; must exceed body limit (JSON escaping can expand a body) |
| `--inflight-window` | `RELAY_INFLIGHT_WINDOW` | `32` | Delivered-but-unacked messages per connection |
| `--outbound-queue` | `RELAY_OUTBOUND_QUEUE` | `256` | Queued replies per connection; a client that never reads them is disconnected |
| `--dedupe-window` | `RELAY_DEDUPE_WINDOW` | `1000` | Acked messages remembered per recipient so a late retry is still recognised as a duplicate (8 bytes each; `0` = off) |
| `--register-timeout-ms` | `RELAY_REGISTER_TIMEOUT_MS` | `10000` | A connection must `REGISTER` within this long (PINGs do not extend it) |
| `--idle-timeout-ms` | `RELAY_IDLE_TIMEOUT_MS` | `60000` | Close a registered connection that sends nothing for this long (send `PING` to stay alive) |
| `--shutdown-timeout-ms` | `RELAY_SHUTDOWN_TIMEOUT_MS` | `5000` | Max time graceful shutdown waits for connections |

Stored message bodies are capped by `max-stored-bytes`, shared by every mailbox and
released as messages are acknowledged. The per-mailbox count limit keeps one recipient
from using the whole budget's slots. Per-connection buffers add at most about
`max-connections × (max-frame-bytes + outbound-queue frames)` on top. The dedupe window adds at
most `max-clients × dedupe-window × 8` bytes (about 80 MB at the defaults, only for
recipients that have acked enough messages to fill it).

## Server lifecycle

- **Start:** binds immediately, logs `relay listening on host:port` with the effective config.
- **Stop:** Ctrl-C / SIGTERM. The server stops accepting, sends `{"type":"SHUTDOWN"}` to
  every client, gives each up to 1 s to flush, then force-closes anything left within
  `shutdown-timeout-ms`. Exit code is 143 (the JVM's normal code for SIGTERM).
- **State is in memory only:** queued messages are lost when the server stops.
- In tests, `RelayServer` is started on port `0` and closed with `close()` (it is
  `AutoCloseable`). No fixed ports, so test runs never clash.

## Dependencies

| Dependency | Scope | Why |
|---|---|---|
| `jackson-databind` 2.18.2 | runtime | JSON encoding/decoding of frames |
| `junit-jupiter` 5.11.4 | test | tests |

Networking is plain `java.net` sockets with virtual threads. No messaging or networking framework is used.

## CI

`.github/workflows/ci.yml` runs `./mvnw -B verify`, uploads `relay.jar` as a build
artifact, and builds the Docker image.

## Project layout

```
src/main/java/relay/
  Main.java          entry point, arg parsing, shutdown hook
  Config.java        all limits/timeouts (flags > env > defaults)
  RelayServer.java   accept loop, connection limit, lifecycle
  Connection.java    per-connection reader + writer threads, request handling
  FrameReader.java   bounded newline framing
  Protocol.java      frame types, error codes, JSON encode/decode
  Registry.java      name -> ClientRecord
  ClientRecord.java  logical client: mailbox + current session, one lock
  Mailbox.java       delivery state machine (pure logic)
  StorageBudget.java server-wide byte cap shared by all mailboxes (lock-free)
  Message.java
  cli/RelayClient.java  demo client
src/test/java/relay/
  MailboxTest.java, ConfigTest.java      unit tests
  RelayIntegrationTest.java              real-socket end-to-end tests
  TestClient.java                        timeout-bounded test client
```
