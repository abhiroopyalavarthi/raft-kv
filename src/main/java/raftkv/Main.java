package raftkv;

import raftkv.bench.Benchmark;
import raftkv.bench.Demo;
import raftkv.bench.LocalCluster;
import raftkv.client.ClusterSpec;
import raftkv.client.KvCli;
import raftkv.kv.KvNode;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftEnv;
import raftkv.sim.SimMain;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

public final class Main {
    private static final String USAGE = """
            usage: raft-kv <command> [options]

              cluster [--nodes 5] [--fresh] [--no-batching]   start a local cluster (one process per node)
              client [put <k> <v> | get <k> | delete <k> | status]   talk to the cluster (no args: shell)
              node --id N [--cluster SPEC] [--data DIR] [--no-batching] [--verbose]   run one node
              sim [--seeds N] [--seed S --verbose] [--bug NAME]   randomized fault-injection runs
              demo                                             kill the leader under load, verify no lost writes
              bench [--clients 32] [--seconds 10] [--trials 5] throughput, latency and failover numbers
            """;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.print(USAGE);
            return;
        }
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "node" -> node(rest);
            case "cluster" -> cluster(rest);
            case "client" -> KvCli.main(rest);
            case "sim" -> SimMain.main(rest);
            case "demo" -> Demo.main(rest);
            case "bench" -> Benchmark.main(rest);
            default -> {
                System.out.print(USAGE);
                System.exit(2);
            }
        }
    }

    private static void node(String[] args) throws Exception {
        int id = 0;
        String spec = null;
        Path data = null;
        boolean batching = true;
        int logLevel = RaftEnv.LOG_INFO;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id" -> id = Integer.parseInt(args[++i]);
                case "--cluster" -> spec = args[++i];
                case "--data" -> data = Path.of(args[++i]);
                case "--no-batching" -> batching = false;
                case "--verbose" -> logLevel = RaftEnv.LOG_DEBUG;
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        if (id <= 0) {
            throw new IllegalArgumentException("--id is required");
        }
        Map<Integer, InetSocketAddress> cluster = spec != null ? ClusterSpec.parse(spec)
                : ClusterSpec.local(5, ClusterSpec.DEFAULT_BASE_PORT);
        if (data == null) {
            data = Path.of("data", "node-" + id);
        }
        RaftConfig cfg = RaftConfig.defaults().withBatching(batching);
        KvNode node = new KvNode(id, cluster, data, cfg, logLevel, System.out::println);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close));
        node.start();
        System.out.println("n" + id + " listening on " + cluster.get(id) + ", data in " + data.toAbsolutePath());
        Thread.currentThread().join();
    }

    private static void cluster(String[] args) throws Exception {
        int nodes = 5;
        boolean fresh = false;
        boolean batching = true;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--nodes" -> nodes = Integer.parseInt(args[++i]);
                case "--fresh" -> fresh = true;
                case "--no-batching" -> batching = false;
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        LocalCluster cluster = new LocalCluster(nodes, ClusterSpec.DEFAULT_BASE_PORT, Path.of("data"), batching, true);
        if (fresh) {
            cluster.wipe();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(cluster::close));
        cluster.start();
        System.out.println("started " + nodes + " nodes on 127.0.0.1:" + ClusterSpec.DEFAULT_BASE_PORT + "-"
                + (ClusterSpec.DEFAULT_BASE_PORT + nodes - 1) + "; Ctrl-C stops them all");
        for (int i = 1; i <= nodes; i++) {
            System.out.println("  n" + i + " pid " + cluster.pid(i));
        }
        Thread.currentThread().join();
    }
}
