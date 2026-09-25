package raftkv.bench;

import raftkv.client.ClusterSpec;
import raftkv.client.KvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/** Runs N nodes as separate JVM processes on this machine. */
public final class LocalCluster implements AutoCloseable {
    private final int nodes;
    private final Map<Integer, InetSocketAddress> addresses;
    private final Path dataDir;
    private final boolean batching;
    private final boolean echo;
    private final Map<Integer, Process> processes = new TreeMap<>();

    public LocalCluster(int nodes, int basePort, Path dataDir, boolean batching, boolean echoOutput) {
        this.nodes = nodes;
        this.addresses = ClusterSpec.local(nodes, basePort);
        this.dataDir = dataDir;
        this.batching = batching;
        this.echo = echoOutput;
    }

    /** Deletes all node data so the cluster starts empty. */
    public LocalCluster wipe() throws IOException {
        if (Files.exists(dataDir)) {
            try (Stream<Path> walk = Files.walk(dataDir)) {
                for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(p);
                }
            }
        }
        return this;
    }

    public void start() throws IOException {
        Files.createDirectories(dataDir);
        for (int i = 1; i <= nodes; i++) {
            startNode(i);
        }
    }

    public void startNode(int id) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> cmd = new ArrayList<>(List.of(java, "-Xmx256m", "-XX:+UseSerialGC",
                "-cp", System.getProperty("java.class.path"), "raftkv.Main", "node",
                "--id", String.valueOf(id),
                "--cluster", ClusterSpec.format(addresses),
                "--data", dataDir.resolve("node-" + id).toString()));
        if (!batching) {
            cmd.add("--no-batching");
        }
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        if (!echo) {
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(dataDir.resolve("node-" + id + ".log").toFile()));
        }
        Process p = pb.start();
        processes.put(id, p);
        if (echo) {
            Thread t = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        System.out.println("[n" + id + "] " + line);
                    }
                } catch (IOException ignored) {
                    // process ended
                }
            }, "echo-n" + id);
            t.setDaemon(true);
            t.start();
        }
    }

    /** kill -9: no shutdown hooks, no flushing. Only what was fsynced survives. */
    public void kill(int id) throws InterruptedException {
        Process p = processes.remove(id);
        if (p != null) {
            p.destroyForcibly();
            p.waitFor();
        }
    }

    public long pid(int id) {
        Process p = processes.get(id);
        return p == null ? -1 : p.pid();
    }

    public boolean isRunning(int id) {
        Process p = processes.get(id);
        return p != null && p.isAlive();
    }

    public Map<Integer, InetSocketAddress> addresses() {
        return addresses;
    }

    public int size() {
        return nodes;
    }

    /** Waits until a single node reports itself leader and returns its id. */
    public int awaitLeader(long timeoutMs) throws InterruptedException {
        return awaitLeader(timeoutMs, -1);
    }

    public int awaitLeader(long timeoutMs, int excluding) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        try (KvClient c = new KvClient(addresses).withTimeouts(300, 1000)) {
            while (System.currentTimeMillis() < deadline) {
                for (Map.Entry<Integer, String> e : c.statusAll().entrySet()) {
                    Map<String, String> s = parseStatus(e.getValue());
                    if ("LEADER".equals(s.get("role")) && e.getKey() != excluding) {
                        return e.getKey();
                    }
                }
                Thread.sleep(20);
            }
        }
        throw new IllegalStateException("no leader within " + timeoutMs + " ms");
    }

    public static Map<String, String> parseStatus(String status) {
        Map<String, String> m = new HashMap<>();
        if (status == null) {
            return m;
        }
        for (String kv : status.split(" ")) {
            int eq = kv.indexOf('=');
            if (eq > 0) {
                m.put(kv.substring(0, eq), kv.substring(eq + 1));
            }
        }
        return m;
    }

    @Override
    public void close() {
        for (Process p : processes.values()) {
            p.destroyForcibly();
        }
        for (Process p : processes.values()) {
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        processes.clear();
    }
}
