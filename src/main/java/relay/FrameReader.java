package relay;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Reads '\n'-terminated UTF-8 frames, refusing to buffer more than {@code maxBytes}.
 * This is the framing bound: a client cannot make the server hold an unbounded line.
 */
final class FrameReader {

    static final class FrameTooLargeException extends IOException {
        FrameTooLargeException(int max) {
            super("frame exceeds " + max + " bytes");
        }
    }

    private final InputStream in; // expected to be buffered
    private final int maxBytes;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    FrameReader(InputStream in, int maxBytes) {
        this.in = in;
        this.maxBytes = maxBytes;
    }

    /** Next frame without the terminator, or null on clean EOF between frames. */
    String read() throws IOException {
        buffer.reset();
        while (true) {
            int b = in.read();
            if (b == -1) {
                if (buffer.size() == 0) {
                    return null;
                }
                throw new EOFException("connection closed mid-frame");
            }
            if (b == '\n') {
                byte[] bytes = buffer.toByteArray();
                int len = bytes.length;
                if (len > 0 && bytes[len - 1] == '\r') {
                    len--; // tolerate CRLF (e.g. telnet)
                }
                return new String(bytes, 0, len, StandardCharsets.UTF_8);
            }
            if (buffer.size() >= maxBytes) {
                throw new FrameTooLargeException(maxBytes);
            }
            buffer.write(b);
        }
    }
}
