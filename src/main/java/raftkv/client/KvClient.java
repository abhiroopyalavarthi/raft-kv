package raftkv.client;

import raftkv.kv.Op;
import raftkv.transport.Codec;
import raftkv.transport.Message;
import raftkv.transport.Message.ClientReply;
import raftkv.transport.Message.ClientRequest;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Blocking client for a real cluster. Finds the leader, follows redirects and retries until the
 * operation succeeds. Every attempt of one operation carries the same (clientId, seq), so a write
 * that was applied but whose reply got lost is not applied again. Not thread-safe: use one per thread.
 */
public final class KvClient implements AutoCloseable {

    /** What an operation returned and which node answered it. */
    public record Reply(String value, int servedBy) {}

    private final TreeMap<Integer, InetSocketAddress> nodes;
    private final int[] ids;
    private final long clientId;
    private final Map<Integer, Conn> conns = new HashMap<>();
    private long seq;
    private int leaderGuess;
    private int rotation;
    private int attemptTimeoutMs = 1000;
    private long operationTimeoutMs = 30_000;

    public KvClient(Map<Integer, InetSocketAddress> cluster) {
        this.nodes = new TreeMap<>(cluster);
        this.ids = nodes.keySet().stream().mapToInt(Integer::intValue).toArray();
        this.clientId = new SecureRandom().nextLong() & Long.MAX_VALUE;
    }

    public KvClient withTimeouts(int attemptMs, long operationMs) {
        this.attemptTimeoutMs = attemptMs;
        this.operationTimeoutMs = operationMs;
        return this;
    }

    public void put(String key, String value) throws IOException {
        call(Op.PUT, key, value);
    }

    public String get(String key) throws IOException {
        return call(Op.GET, key, null).value();
    }

    public void delete(String key) throws IOException {
        call(Op.DELETE, key, null);
    }

    public Reply call(Op op, String key, String value) throws IOException {
        long s = ++seq;
        ClientRequest req = new ClientRequest(0, 0, clientId, s, op, key, value);
        long deadline = System.currentTimeMillis() + operationTimeoutMs;
        IOException last = null;
        while (System.currentTimeMillis() < deadline) {
            int target = leaderGuess != 0 ? leaderGuess : ids[rotation++ % ids.length];
            try {
                ClientReply r = conn(target).request(req, attemptTimeoutMs);
                if (r.status() == Message.Status.OK) {
                    leaderGuess = target;
                    return new Reply(r.value(), target);
                }
                // NOT_LEADER: follow the hint if there is one, otherwise wait out the election a little
                leaderGuess = r.leaderHint() != 0 && r.leaderHint() != target ? r.leaderHint() : 0;
                if (leaderGuess == 0) {
                    pause(20);
                }
            } catch (IOException e) {
                last = e;
                closeConn(target);
                leaderGuess = 0;
                pause(5);
            }
        }
        throw new IOException("gave up on " + op + " " + key + " after " + operationTimeoutMs + " ms", last);
    }

    /** Asks one specific node for its Raft status (no redirects). */
    public String status(int nodeId) throws IOException {
        ClientRequest req = new ClientRequest(0, 0, clientId, ++seq, Op.STATUS, "", null);
        try {
            return conn(nodeId).request(req, attemptTimeoutMs).value();
        } catch (IOException e) {
            closeConn(nodeId);
            throw e;
        }
    }

    /** Status of each node, or null for nodes that did not answer. */
    public Map<Integer, String> statusAll() {
        Map<Integer, String> out = new TreeMap<>();
        for (int id : ids) {
            try {
                out.put(id, status(id));
            } catch (IOException e) {
                out.put(id, null);
            }
        }
        return out;
    }

    private Conn conn(int id) throws IOException {
        Conn c = conns.get(id);
        if (c == null) {
            c = new Conn(nodes.get(id));
            conns.put(id, c);
        }
        return c;
    }

    private void closeConn(int id) {
        Conn c = conns.remove(id);
        if (c != null) {
            c.close();
        }
    }

    @Override
    public void close() {
        for (Conn c : conns.values()) {
            c.close();
        }
        conns.clear();
    }

    private static void pause(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class Conn {
        final Socket socket;
        final DataOutputStream out;
        final DataInputStream in;

        Conn(InetSocketAddress addr) throws IOException {
            socket = new Socket();
            try {
                socket.connect(addr, 300);
                socket.setTcpNoDelay(true);
                out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            } catch (IOException e) {
                socket.close();
                throw e;
            }
        }

        ClientReply request(ClientRequest req, int timeoutMs) throws IOException {
            byte[] b = Codec.encode(req);
            out.writeInt(b.length);
            out.write(b);
            out.flush();
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (true) {
                int left = (int) (deadline - System.currentTimeMillis());
                if (left <= 0) {
                    throw new SocketTimeoutException("no reply within " + timeoutMs + " ms");
                }
                socket.setSoTimeout(left);
                int len = in.readInt();
                byte[] frame = new byte[len];
                in.readFully(frame);
                if (Codec.decode(frame) instanceof ClientReply r && r.seq() == req.seq() && r.clientId() == req.clientId()) {
                    return r;
                }
                // a late reply to an earlier attempt: ignore it
            }
        }

        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // already closed
            }
        }
    }
}
