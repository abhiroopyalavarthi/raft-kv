package raftkv.transport;

import raftkv.transport.Message.ClientRequest;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * TCP transport for one node.
 *
 * <p>Each node keeps one outbound connection to every peer and only writes on it; peers' inbound
 * connections are only read. Clients connect to any node and get replies on the same socket: the
 * transport gives each client connection an endpoint id and routes ClientReply messages back by it.
 *
 * <p>Raft tolerates lost messages, so this layer never retries: if a peer is unreachable its queued
 * messages are dropped and the connection is retried in the background. Incoming messages are handed
 * to {@code inbox}, which must pass them to the node's own thread.
 */
public final class TcpTransport implements Transport, AutoCloseable {
    private static final int QUEUE_CAPACITY = 50_000;
    private static final int MAX_FRAME = 64 * 1024 * 1024;

    private final int selfId;
    private final int listenPort;
    private final Map<Integer, InetSocketAddress> peers;
    private final Consumer<Message> inbox;
    private final Map<Integer, Link> peerLinks = new ConcurrentHashMap<>();
    private final Map<Integer, Link> clientLinks = new ConcurrentHashMap<>();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final AtomicInteger nextClientEndpoint = new AtomicInteger(1_000_000);
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean closed;
    private ServerSocket server;

    public TcpTransport(int selfId, int listenPort, Map<Integer, InetSocketAddress> peers, Consumer<Message> inbox) {
        this.selfId = selfId;
        this.listenPort = listenPort;
        this.peers = Map.copyOf(peers);
        this.inbox = inbox;
    }

    public void start() throws IOException {
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(listenPort));
        spawn("accept-" + selfId, this::acceptLoop);
        for (Map.Entry<Integer, InetSocketAddress> p : peers.entrySet()) {
            PeerLink link = new PeerLink(p.getKey(), p.getValue());
            peerLinks.put(p.getKey(), link);
            spawn("to-n" + p.getKey(), link::run);
        }
    }

    @Override
    public void send(Message m) {
        if (closed) {
            return;
        }
        Link link = m.to() <= 1000 ? peerLinks.get(m.to()) : clientLinks.get(m.to());
        if (link != null) {
            link.offer(Codec.encode(m));
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            if (server != null) {
                server.close();
            }
        } catch (IOException ignored) {
            // shutting down
        }
        for (Socket s : sockets) {
            closeQuietly(s);
        }
        for (Thread t : threads) {
            t.interrupt();
        }
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket s = server.accept();
                s.setTcpNoDelay(true);
                sockets.add(s);
                spawn("from-" + s.getRemoteSocketAddress(), () -> readLoop(s));
            } catch (IOException e) {
                if (!closed) {
                    sleep(10);
                }
            }
        }
    }

    private void readLoop(Socket s) {
        int endpoint = 0;
        SocketLink replyLink = null;
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 1 << 16))) {
            while (!closed) {
                byte[] frame = readFrame(in);
                Message m = Codec.decode(frame);
                if (m instanceof ClientRequest req) {
                    if (endpoint == 0) {
                        endpoint = nextClientEndpoint.getAndIncrement();
                        replyLink = new SocketLink(s);
                        clientLinks.put(endpoint, replyLink);
                        spawn("reply-" + endpoint, replyLink::run);
                    }
                    m = req.withEndpoints(endpoint, selfId);
                }
                inbox.accept(m);
            }
        } catch (IOException | RuntimeException e) {
            // connection closed or garbage on the wire: drop the connection
        } finally {
            if (endpoint != 0) {
                clientLinks.remove(endpoint);
                replyLink.close();
            }
            sockets.remove(s);
            closeQuietly(s);
        }
    }

    static byte[] readFrame(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > MAX_FRAME) {
            throw new IOException("bad frame length " + len);
        }
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
    }

    static void writeFrame(DataOutputStream out, byte[] b) throws IOException {
        out.writeInt(b.length);
        out.write(b);
    }

    /** A queue drained by one writer thread. */
    private abstract class Link {
        final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        void offer(byte[] frame) {
            queue.offer(frame); // full means the peer is far behind or gone; Raft will resend
        }

        void writeBatch(DataOutputStream out) throws IOException, InterruptedException {
            byte[] first = queue.poll(200, TimeUnit.MILLISECONDS);
            if (first == null) {
                return;
            }
            List<byte[]> batch = new ArrayList<>();
            batch.add(first);
            queue.drainTo(batch, 1024);
            for (byte[] b : batch) {
                writeFrame(out, b);
            }
            out.flush();
        }
    }

    private final class PeerLink extends Link {
        private final int peerId;
        private final InetSocketAddress address;

        PeerLink(int peerId, InetSocketAddress address) {
            this.peerId = peerId;
            this.address = address;
        }

        void run() {
            long backoff = 20;
            while (!closed) {
                Socket s = new Socket();
                try {
                    s.connect(address, 500);
                    s.setTcpNoDelay(true);
                    sockets.add(s);
                    backoff = 20;
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 1 << 16));
                    while (!closed) {
                        writeBatch(out);
                    }
                } catch (IOException e) {
                    queue.clear(); // stale heartbeats are useless once the peer comes back
                    sleep(backoff);
                    backoff = Math.min(backoff * 2, 200);
                } catch (InterruptedException e) {
                    return;
                } finally {
                    sockets.remove(s);
                    closeQuietly(s);
                }
            }
        }

        @Override
        public String toString() {
            return "link to n" + peerId;
        }
    }

    private final class SocketLink extends Link {
        private final Socket socket;
        private volatile boolean done;

        SocketLink(Socket socket) {
            this.socket = socket;
        }

        void run() {
            try {
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 1 << 16));
                while (!closed && !done) {
                    writeBatch(out);
                }
            } catch (IOException | InterruptedException e) {
                closeQuietly(socket);
            }
        }

        void close() {
            done = true;
        }
    }

    private void spawn(String name, Runnable r) {
        Thread t = new Thread(r, "n" + selfId + "-" + name);
        t.setDaemon(true);
        synchronized (threads) {
            threads.removeIf(x -> !x.isAlive());
            threads.add(t);
        }
        t.start();
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // already closed
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
