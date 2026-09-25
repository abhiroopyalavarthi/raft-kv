package raftkv.bench;

import org.HdrHistogram.Histogram;
import raftkv.client.KvClient;
import raftkv.kv.Op;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Starts a real 5-process cluster (once without batching, once with) and measures:
 * write throughput and latency, read latency, and leader failover time under load.
 * Results go to docs/results.md.
 * <pre>
 *   raft-kv bench [--clients 32] [--seconds 10] [--trials 5] [--out docs/results.md]
 * </pre>
 */
public final class Benchmark {

    record LoadResult(double opsPerSec, Histogram latencyMicros, long ops, long errors) {}

    record ModeResult(String name, LoadResult writes, LoadResult reads, List<Long> failoversMs) {}

    public static void main(String[] args) throws Exception {
        int clients = 32;
        int seconds = 10;
        int trials = 5;
        Path out = Path.of("docs", "results.md");
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--clients" -> clients = Integer.parseInt(args[++i]);
                case "--seconds" -> seconds = Integer.parseInt(args[++i]);
                case "--trials" -> trials = Integer.parseInt(args[++i]);
                case "--out" -> out = Path.of(args[++i]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        List<ModeResult> results = new ArrayList<>();
        results.add(runMode("No batching (1 entry per fsync and per AppendEntries)", false, clients, seconds, trials));
        results.add(runMode("Batching + pipelining", true, clients, seconds, trials));
        String report = report(results, clients, seconds, trials);
        System.out.println();
        System.out.println(report);
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, report);
        System.out.println("written to " + out);
    }

    static ModeResult runMode(String name, boolean batching, int clients, int seconds, int trials) throws Exception {
        System.out.println("=== " + name);
        Path dir = Path.of("data", batching ? "bench-batched" : "bench-unbatched");
        try (LocalCluster cluster = new LocalCluster(5, 7201, dir, batching, false).wipe()) {
            cluster.start();
            int leader = cluster.awaitLeader(20_000);
            System.out.println("leader n" + leader + "; warming up");
            load(cluster, Op.PUT, clients, 3);
            System.out.printf("writes: %d clients for %d s%n", clients, seconds);
            LoadResult writes = load(cluster, Op.PUT, clients, seconds);
            System.out.printf("  %.0f ops/s, p50 %.2f ms, p99 %.2f ms%n", writes.opsPerSec(),
                    ms(writes.latencyMicros().getValueAtPercentile(50)), ms(writes.latencyMicros().getValueAtPercentile(99)));
            System.out.printf("reads: %d clients for %d s%n", clients, Math.max(2, seconds / 2));
            LoadResult reads = load(cluster, Op.GET, clients, Math.max(2, seconds / 2));
            System.out.printf("  %.0f ops/s, p50 %.2f ms, p99 %.2f ms%n", reads.opsPerSec(),
                    ms(reads.latencyMicros().getValueAtPercentile(50)), ms(reads.latencyMicros().getValueAtPercentile(99)));
            List<Long> failovers = new ArrayList<>();
            for (int t = 1; t <= trials; t++) {
                long f = failover(cluster);
                failovers.add(f);
                System.out.printf("failover trial %d: %d ms%n", t, f);
            }
            return new ModeResult(name, writes, reads, failovers);
        }
    }

    /** Closed-loop load: each client issues its next operation as soon as the last one returns. */
    static LoadResult load(LocalCluster cluster, Op op, int clients, int seconds) throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        List<Histogram> hists = Collections.synchronizedList(new ArrayList<>());
        AtomicLong ops = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        String value = "x".repeat(100);
        for (int c = 0; c < clients; c++) {
            int seed = c;
            Thread t = new Thread(() -> {
                Histogram h = new Histogram(60_000_000L, 3);
                Random rnd = new Random(seed);
                try (KvClient client = new KvClient(cluster.addresses()).withTimeouts(1000, 30_000)) {
                    while (!stop.get()) {
                        String key = "key-" + rnd.nextInt(10_000);
                        long t0 = System.nanoTime();
                        try {
                            client.call(op, key, op == Op.PUT ? value : null);
                            h.recordValue(Math.max(1, (System.nanoTime() - t0) / 1000));
                            ops.incrementAndGet();
                        } catch (IOException e) {
                            errors.incrementAndGet();
                        }
                    }
                }
                hists.add(h);
            });
            threads.add(t);
            t.start();
        }
        long start = System.nanoTime();
        Thread.sleep(seconds * 1000L);
        long counted = ops.get();
        double elapsed = (System.nanoTime() - start) / 1e9;
        stop.set(true);
        for (Thread t : threads) {
            t.join();
        }
        Histogram total = new Histogram(60_000_000L, 3);
        for (Histogram h : hists) {
            total.add(h);
        }
        return new LoadResult(counted / elapsed, total, counted, errors.get());
    }

    /**
     * Kills the leader under write load and returns the time from the kill until the first write
     * is acknowledged by a different node. Then restarts the old leader and lets it catch up.
     */
    static long failover(LocalCluster cluster) throws Exception {
        int leader = cluster.awaitLeader(10_000);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong killTime = new AtomicLong(Long.MAX_VALUE);
        AtomicLong firstNewLeaderAck = new AtomicLong(Long.MAX_VALUE);
        List<Thread> writers = new ArrayList<>();
        for (int c = 0; c < 8; c++) {
            int id = c;
            Thread t = new Thread(() -> {
                try (KvClient client = new KvClient(cluster.addresses()).withTimeouts(500, 30_000)) {
                    for (int i = 0; !stop.get(); i++) {
                        KvClient.Reply r = client.call(Op.PUT, "fo-" + id + "-" + i, "v");
                        long now = System.nanoTime();
                        if (now > killTime.get() && r.servedBy() != leader) {
                            firstNewLeaderAck.accumulateAndGet(now, Math::min);
                        }
                    }
                } catch (IOException e) {
                    System.out.println("writer error: " + e);
                }
            });
            writers.add(t);
            t.start();
        }
        Thread.sleep(1000);
        killTime.set(System.nanoTime());
        cluster.kill(leader);
        long deadline = System.currentTimeMillis() + 15_000;
        while (firstNewLeaderAck.get() == Long.MAX_VALUE && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        stop.set(true);
        for (Thread t : writers) {
            t.join();
        }
        long ms = (firstNewLeaderAck.get() - killTime.get()) / 1_000_000;
        cluster.startNode(leader);
        Thread.sleep(2500); // restarted node catches up before the next trial
        return ms;
    }

    static String report(List<ModeResult> results, int clients, int seconds, int trials) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Benchmark results\n\n");
        sb.append("Measured ").append(LocalDate.now()).append(" with `./gradlew bench`: 5 node processes on one machine over ")
                .append("localhost TCP, every log append fsynced before replying.\n\n");
        sb.append("- Machine: ").append(System.getProperty("os.name")).append(' ').append(System.getProperty("os.arch"))
                .append(", ").append(Runtime.getRuntime().availableProcessors()).append(" CPUs, Java ")
                .append(System.getProperty("java.version")).append('\n');
        sb.append("- Load: ").append(clients).append(" closed-loop clients (each waits for its reply before sending the next), ")
                .append(seconds).append(" s of writes (100-byte values, 10,000 keys) after a 3 s warm-up, then ")
                .append(Math.max(2, seconds / 2)).append(" s of reads\n");
        sb.append("- Reads are linearizable (ReadIndex: the leader confirms it still has a majority before answering)\n");
        sb.append("- Failover: leader killed with SIGKILL while 8 clients write; time until the first write is acknowledged by the new leader (")
                .append(trials).append(" trials)\n\n");
        sb.append("| Configuration | Write throughput | Write p50 | Write p99 | Read throughput | Read p50 | Read p99 | Failover (median) | Failover (max) |\n");
        sb.append("|---|---|---|---|---|---|---|---|---|\n");
        for (ModeResult r : results) {
            List<Long> f = new ArrayList<>(r.failoversMs());
            Collections.sort(f);
            sb.append(String.format("| %s | %,.0f ops/s | %.2f ms | %.2f ms | %,.0f ops/s | %.2f ms | %.2f ms | %d ms | %d ms |%n",
                    r.name(),
                    r.writes().opsPerSec(), ms(r.writes().latencyMicros().getValueAtPercentile(50)),
                    ms(r.writes().latencyMicros().getValueAtPercentile(99)),
                    r.reads().opsPerSec(), ms(r.reads().latencyMicros().getValueAtPercentile(50)),
                    ms(r.reads().latencyMicros().getValueAtPercentile(99)),
                    f.get(f.size() / 2), f.get(f.size() - 1)));
        }
        if (results.size() == 2) {
            double speedup = results.get(1).writes().opsPerSec() / results.get(0).writes().opsPerSec();
            sb.append(String.format("%nBatching raises write throughput %.1fx. ", speedup));
        }
        sb.append("Failover time is dominated by the randomized election timeout (150-300 ms): followers wait that long "
                + "without a heartbeat before starting an election.\n");
        return sb.toString();
    }

    static double ms(long micros) {
        return micros / 1000.0;
    }
}
