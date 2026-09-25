package raftkv.bench;

import raftkv.client.KvClient;
import raftkv.kv.Op;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The leader-kill demo on a real 5-process cluster:
 * a client writes nonstop, the leader is killed with SIGKILL, a new leader takes over, the dead
 * node restarts and catches up, and finally every acknowledged write is read back.
 */
public final class Demo {

    public static void main(String[] args) throws Exception {
        int basePort = 7101;
        long killAtMs = 3000;
        long restartAtMs = 6000;
        long stopAtMs = 9000;
        Path dataDir = Path.of("data", "demo");
        try (LocalCluster cluster = new LocalCluster(5, basePort, dataDir, true, false).wipe()) {
            long t0 = System.currentTimeMillis();
            say(t0, "starting 5 node processes on ports " + basePort + "-" + (basePort + 4));
            cluster.start();
            int leader = cluster.awaitLeader(15_000);
            say(t0, "n" + leader + " is leader (pid " + cluster.pid(leader) + ")");

            List<String> acked = new ArrayList<>();
            List<long[]> completions = new ArrayList<>(); // {time, servedBy}
            AtomicBoolean stop = new AtomicBoolean();
            long writerStart = System.currentTimeMillis();
            Thread writer = new Thread(() -> {
                try (KvClient c = new KvClient(cluster.addresses()).withTimeouts(500, 30_000)) {
                    for (int i = 0; !stop.get(); i++) {
                        String key = "demo-" + i;
                        KvClient.Reply r = c.call(Op.PUT, key, "value-" + i);
                        synchronized (acked) {
                            acked.add(key);
                            completions.add(new long[]{System.currentTimeMillis(), r.servedBy()});
                        }
                    }
                } catch (Exception e) {
                    System.out.println("writer failed: " + e);
                }
            }, "writer");
            writer.start();
            say(t0, "client writing demo-0, demo-1, ... as fast as the cluster acknowledges");

            long nextReport = 1000;
            boolean killed = false;
            boolean restarted = false;
            long killTime = 0;
            int victim = leader;
            int newLeader = -1;
            while (true) {
                long el = System.currentTimeMillis() - writerStart;
                if (el >= nextReport) {
                    int n;
                    synchronized (acked) {
                        n = acked.size();
                    }
                    say(t0, String.format("  %,d writes acknowledged", n));
                    nextReport += 1000;
                }
                if (!killed && el >= killAtMs) {
                    victim = cluster.awaitLeader(5000);
                    say(t0, ">>> kill -9 leader n" + victim + " (pid " + cluster.pid(victim) + ")");
                    killTime = System.currentTimeMillis();
                    cluster.kill(victim);
                    killed = true;
                }
                if (killed && newLeader < 0) {
                    newLeader = cluster.awaitLeader(10_000, victim);
                    long elected = System.currentTimeMillis() - killTime;
                    say(t0, ">>> n" + newLeader + " elected leader " + elected + " ms after the kill");
                }
                if (killed && !restarted && el >= restartAtMs) {
                    say(t0, ">>> restarting n" + victim + " from its data directory");
                    cluster.startNode(victim);
                    restarted = true;
                }
                if (el >= stopAtMs) {
                    break;
                }
                Thread.sleep(10);
            }
            stop.set(true);
            writer.join();

            long firstAfter = -1;
            long lastBefore = -1;
            synchronized (acked) {
                for (long[] c : completions) {
                    if (c[0] <= killTime && c[1] == victim) {
                        lastBefore = c[0];
                    }
                    if (c[0] > killTime && c[1] != victim && firstAfter < 0) {
                        firstAfter = c[0];
                    }
                }
            }
            if (firstAfter > 0 && lastBefore > 0) {
                say(t0, "writes paused for " + (firstAfter - lastBefore)
                        + " ms (last ack from old leader -> first ack from new leader)");
            }

            // Wait for the restarted node to catch up.
            try (KvClient c = new KvClient(cluster.addresses()).withTimeouts(500, 10_000)) {
                long deadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < deadline) {
                    Map<Integer, String> st = c.statusAll();
                    long leaderCommit = Long.parseLong(LocalCluster.parseStatus(st.get(newLeader)).getOrDefault("commit", "0"));
                    Map<String, String> v = LocalCluster.parseStatus(st.get(victim));
                    if (!v.isEmpty() && Long.parseLong(v.get("applied")) >= leaderCommit) {
                        say(t0, ">>> n" + victim + " caught up: applied index " + v.get("applied")
                                + ", role " + v.get("role") + ", following n" + v.get("leader"));
                        break;
                    }
                    Thread.sleep(50);
                }

                say(t0, "reading back every acknowledged write through the cluster...");
                int missing = 0;
                for (int i = 0; i < acked.size(); i++) {
                    String val = c.get(acked.get(i));
                    String expected = "value-" + acked.get(i).substring("demo-".length());
                    if (!expected.equals(val)) {
                        missing++;
                    }
                }
                say(t0, String.format("acknowledged writes: %,d   lost or wrong: %d", acked.size(), missing));
                for (Map.Entry<Integer, String> e : c.statusAll().entrySet()) {
                    say(t0, "  n" + e.getKey() + ": " + e.getValue());
                }
                if (missing > 0) {
                    System.exit(1);
                }
            }
        }
    }

    private static void say(long t0, String msg) {
        System.out.printf("[%5.2fs] %s%n", (System.currentTimeMillis() - t0) / 1000.0, msg);
    }
}
