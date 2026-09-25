package raftkv.kv;

import raftkv.raft.FileStorage;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftEnv;
import raftkv.transport.Message;
import raftkv.transport.TcpTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A real node: file storage, TCP transport, and one thread that runs every Raft event in order.
 * Network threads only decode bytes and hand messages to that thread.
 */
public final class KvNode implements AutoCloseable {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final int id;
    private final ScheduledExecutorService loop;
    private final FileStorage storage;
    private final TcpTransport transport;
    private final KvServer server;
    private volatile boolean closed;

    public KvNode(int id, Map<Integer, InetSocketAddress> cluster, Path dataDir, RaftConfig cfg,
                  int logLevel, Consumer<String> logSink) {
        this.id = id;
        this.loop = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-n" + id);
            t.setDaemon(true);
            return t;
        });
        this.storage = new FileStorage(dataDir);
        Map<Integer, InetSocketAddress> peers = new HashMap<>(cluster);
        peers.remove(id);
        int[] members = cluster.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
        this.transport = new TcpTransport(id, cluster.get(id).getPort(), peers, this::deliver);
        long origin = System.nanoTime();
        RaftEnv env = new RaftEnv() {
            private final Random random = new Random();

            @Override
            public long now() {
                return (System.nanoTime() - origin) / 1_000_000;
            }

            @Override
            public Random random() {
                return random;
            }

            @Override
            public void send(Message message) {
                transport.send(message);
            }

            @Override
            public void schedule(long delayMs, Runnable task) {
                if (delayMs <= 0) {
                    execute(task);
                } else if (!closed) {
                    loop.schedule(() -> guarded(task), delayMs, TimeUnit.MILLISECONDS);
                }
            }

            @Override
            public int logLevel() {
                return logLevel;
            }

            @Override
            public void log(String line) {
                logSink.accept(LocalTime.now().format(TIME) + " " + line);
            }
        };
        this.server = new KvServer(id, members, cfg, env, storage);
    }

    private void deliver(Message m) {
        execute(() -> server.onMessage(m));
    }

    public void start() throws IOException {
        transport.start();
        execute(server::start);
    }

    public int id() {
        return id;
    }

    /** Runs on the node thread and waits for the answer (for tests and status checks). */
    public <T> T query(java.util.function.Function<KvServer, T> f) {
        CompletableFuture<T> result = new CompletableFuture<>();
        execute(() -> result.complete(f.apply(server)));
        try {
            return result.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("node n" + id + " did not answer", e);
        }
    }

    private void execute(Runnable task) {
        if (closed) {
            return;
        }
        try {
            loop.execute(() -> guarded(task));
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            // shutting down
        }
    }

    private void guarded(Runnable task) {
        if (closed) {
            return;
        }
        try {
            task.run();
        } catch (Throwable t) {
            // A bug in Raft must not be silently swallowed by the executor: stop the node loudly.
            System.err.println("FATAL on n" + id + ": " + t);
            t.printStackTrace();
            close();
        }
    }

    /** Stops abruptly, like a crash: no goodbye messages. Everything acknowledged is already on disk. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        server.stop();
        transport.close();
        loop.shutdownNow();
        try {
            loop.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        storage.close();
    }
}
