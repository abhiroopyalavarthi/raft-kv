package raftkv.sim;

import raftkv.checker.History;
import raftkv.checker.LinearizabilityChecker;
import raftkv.checker.Operation;
import raftkv.raft.PlantedBug;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftEnv;
import raftkv.raft.RaftNode;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

/**
 * One randomized fault-injection run, fully determined by its seed:
 * <ol>
 *   <li>Random network: delays, loss rate and duplication rate are drawn per run.</li>
 *   <li>Fault phase: at random intervals, crash or restart nodes, partition the network
 *       (sometimes cutting the leader off), heal, or change the loss rate.</li>
 *   <li>Settle phase: heal everything, restart every node, and let clients finish.</li>
 * </ol>
 * The five Raft safety rules are checked after every step; at the end the client history is
 * checked for linearizability, every operation must have finished (liveness once healed), and
 * all nodes must hold the same data.
 */
public final class RandomizedRun {

    public record Options(int nodes, int clients, int keys, long faultPhaseMs, long settlePhaseMs,
                          RaftConfig raft, boolean faults, int logLevel, Consumer<String> logSink,
                          Consumer<String> tracer) {
        public static Options defaults() {
            return new Options(5, 4, 3, 3000, 2500, RaftConfig.defaults(), true, RaftEnv.LOG_OFF, s -> {}, null);
        }

        public Options withRaft(RaftConfig c) {
            return new Options(nodes, clients, keys, faultPhaseMs, settlePhaseMs, c, faults, logLevel, logSink, tracer);
        }

        public Options withBugs(Set<PlantedBug> bugs) {
            return withRaft(raft.withBugs(bugs));
        }

        public Options withLogging(int level, Consumer<String> sink) {
            return new Options(nodes, clients, keys, faultPhaseMs, settlePhaseMs, raft, faults, level, sink, tracer);
        }

        public Options withTracer(Consumer<String> t) {
            return new Options(nodes, clients, keys, faultPhaseMs, settlePhaseMs, raft, faults, logLevel, logSink, t);
        }

        public Options withFaults(boolean f) {
            return new Options(nodes, clients, keys, faultPhaseMs, settlePhaseMs, raft, f, logLevel, logSink, tracer);
        }

        public Options withDurations(long faultMs, long settleMs) {
            return new Options(nodes, clients, keys, faultMs, settleMs, raft, faults, logLevel, logSink, tracer);
        }
    }

    public record Result(long seed, List<String> violations, int operations, int completed, long steps,
                         long leaderElections, long maxTerm, long committed, String faultLog) {
        public boolean ok() {
            return violations.isEmpty();
        }
    }

    public static Result run(long seed, Options o) {
        Simulator sim = new Simulator(seed);
        Random rnd = sim.random();
        SimNetwork net = new SimNetwork(sim);
        if (o.tracer() != null) {
            net.setTracer(o.tracer());
        }
        int minDelay = 1 + rnd.nextInt(3);
        net.setDelay(minDelay, minDelay + 2 + rnd.nextInt(25));
        double baseDrop = o.faults() ? rnd.nextDouble() * 0.1 : 0;
        net.setDropRate(baseDrop);
        net.setDuplicateRate(o.faults() ? rnd.nextDouble() * 0.03 : 0);

        SimCluster cluster = new SimCluster(sim, net, o.nodes(), o.raft());
        InvariantChecker invariants = new InvariantChecker(cluster);
        cluster.setObserver(invariants);
        cluster.setLogging(o.logLevel(), o.logSink());
        StringBuilder faultLog = new StringBuilder();
        Consumer<String> fault = s -> {
            faultLog.append("t=").append(sim.now()).append(' ').append(s).append('\n');
            if (o.logLevel() > 0) {
                o.logSink().accept("t=" + sim.now() + " *** " + s);
            }
        };

        History history = new History();
        String[] keys = new String[o.keys()];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = "k" + i;
        }
        List<SimClient> clients = new ArrayList<>();
        long end = o.faultPhaseMs() + o.settlePhaseMs();
        for (int c = 0; c < o.clients(); c++) {
            SimClient client = new SimClient(100 + c, c + 1, sim, net, history, o.nodes(), keys, 120);
            client.stopIssuingAt(end - 1000);
            clients.add(client);
        }

        cluster.startAll();
        for (SimClient c : clients) {
            c.start(rnd.nextInt(50));
        }
        if (o.faults()) {
            scheduleFault(sim, cluster, net, rnd, o.faultPhaseMs(), fault);
            sim.schedule(o.faultPhaseMs(), () -> {
                fault.accept("heal network, restart all nodes");
                net.heal();
                net.setDropRate(0);
                for (int i = 1; i <= o.nodes(); i++) {
                    cluster.restart(i);
                }
            });
        }

        List<String> violations = new ArrayList<>();
        try {
            sim.runUntil(end, invariants::afterStep, invariants::failed);
        } catch (RuntimeException e) {
            violations.add("exception at t=" + sim.now() + ": " + e);
        }
        violations.addAll(invariants.violations());

        if (violations.isEmpty()) {
            for (SimClient c : clients) {
                if (!c.idle()) {
                    violations.add("Liveness: operation still unfinished " + (end - sim.now() + 1000)
                            + " ms after the network healed: " + c.current());
                    break;
                }
            }
        }
        if (violations.isEmpty()) {
            checkConvergence(cluster, violations);
        }
        List<Operation> ops = history.operations();
        if (violations.isEmpty()) {
            LinearizabilityChecker.Result r = new LinearizabilityChecker().check(ops);
            if (!r.linearizable()) {
                violations.add((r.timedOut() ? "Linearizability check inconclusive: " : "Linearizability violated: ") + r.explanation());
            }
        }
        return new Result(seed, violations, ops.size(), history.completedCount(), sim.steps(),
                invariants.leaderElections(), invariants.maxTerm(), invariants.committedMax(), faultLog.toString());
    }

    private static void checkConvergence(SimCluster cluster, List<String> violations) {
        SimCluster.Host leader = cluster.leader();
        if (leader == null) {
            violations.add("Liveness: no leader at the end of the run");
            return;
        }
        long commit = leader.raft().commitIndex();
        Map<String, String> expected = leader.server().stateMachine().snapshotData();
        for (SimCluster.Host h : cluster.upHosts()) {
            RaftNode n = h.raft();
            if (n.lastApplied() != commit) {
                violations.add("Convergence: n" + n.id() + " applied " + n.lastApplied() + " but leader committed " + commit);
                return;
            }
            if (!h.server().stateMachine().snapshotData().equals(expected)) {
                violations.add("Convergence: n" + n.id() + " holds different data from the leader");
                return;
            }
        }
    }

    private static void scheduleFault(Simulator sim, SimCluster cluster, SimNetwork net, Random rnd,
                                      long until, Consumer<String> log) {
        long delay = 20 + rnd.nextInt(400);
        if (sim.now() + delay >= until) {
            return;
        }
        sim.schedule(delay, () -> {
            injectFault(cluster, net, rnd, log);
            scheduleFault(sim, cluster, net, rnd, until, log);
        });
    }

    private static void injectFault(SimCluster cluster, SimNetwork net, Random rnd, Consumer<String> log) {
        int n = cluster.size();
        int down = cluster.downCount();
        int choice = rnd.nextInt(100);
        if (choice < 25) {
            // crash: usually a minority, occasionally a majority (no progress, but must stay safe)
            if (down < n / 2 || (down == n / 2 && rnd.nextInt(4) == 0)) {
                SimCluster.Host leader = cluster.leader();
                int victim = leader != null && rnd.nextBoolean() ? leader.id : 1 + rnd.nextInt(n);
                if (cluster.host(victim).isUp()) {
                    log.accept("crash n" + victim + (leader != null && leader.id == victim ? " (leader)" : ""));
                    cluster.crash(victim);
                }
            }
        } else if (choice < 50) {
            if (down > 0) {
                List<Integer> dead = new ArrayList<>();
                for (SimCluster.Host h : cluster.hosts()) {
                    if (!h.isUp()) {
                        dead.add(h.id);
                    }
                }
                int id = dead.get(rnd.nextInt(dead.size()));
                log.accept("restart n" + id);
                cluster.restart(id);
            }
        } else if (choice < 70) {
            int[] group = new int[n + 1];
            SimCluster.Host leader = cluster.leader();
            if (leader != null && rnd.nextBoolean()) {
                // cut the leader off, sometimes with one other node
                group[leader.id] = 1;
                if (rnd.nextBoolean()) {
                    group[1 + rnd.nextInt(n)] = 1;
                }
            } else {
                for (int i = 1; i <= n; i++) {
                    group[i] = rnd.nextInt(2);
                }
            }
            StringBuilder sb = new StringBuilder("partition");
            for (int g = 0; g < 2; g++) {
                sb.append(g == 0 ? " {" : " | {");
                for (int i = 1; i <= n; i++) {
                    if (group[i] == g) {
                        sb.append(" n").append(i);
                    }
                }
                sb.append(" }");
            }
            log.accept(sb.toString());
            net.partition(group);
        } else if (choice < 85) {
            log.accept("heal partition");
            net.heal();
        } else if (choice < 95) {
            double p = rnd.nextDouble() * 0.25;
            log.accept(String.format("drop rate %.2f", p));
            net.setDropRate(p);
        }
        // else: quiet period
    }

    public static Set<PlantedBug> noBugs() {
        return EnumSet.noneOf(PlantedBug.class);
    }
}
