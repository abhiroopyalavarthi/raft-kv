package raftkv.sim;

import org.junit.jupiter.api.Test;
import raftkv.checker.History;
import raftkv.checker.LinearizabilityChecker;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Milestone 2: every node applies the same commands in the same order, across repeated leader changes. */
class ReplicationTest {

    static String run(long seed) {
        Simulator sim = new Simulator(seed);
        SimNetwork net = new SimNetwork(sim);
        net.setDelay(1, 15);
        net.setDropRate(0.02);
        SimCluster cluster = new SimCluster(sim, net, 5, RaftConfig.defaults());
        InvariantChecker inv = new InvariantChecker(cluster);
        cluster.setObserver(inv);
        History history = new History();
        List<SimClient> clients = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            SimClient c = new SimClient(100 + i, i + 1, sim, net, history, 5, new String[]{"a", "b", "c", "d"}, 120);
            c.stopIssuingAt(4000);
            clients.add(c);
        }
        cluster.startAll();
        clients.forEach(c -> c.start(0));
        // Kill the leader every 600 ms and bring it back 300 ms later.
        for (long t = 600; t < 3600; t += 600) {
            sim.schedule(t, () -> {
                SimCluster.Host l = cluster.leader();
                if (l != null) {
                    cluster.crash(l.id);
                    sim.schedule(300, () -> cluster.restart(l.id));
                }
            });
        }
        sim.runUntil(5000, inv::afterStep, inv::failed);
        if (inv.failed()) {
            return "seed " + seed + ": " + inv.violations().get(0);
        }
        if (inv.leaderElections() < 5) {
            return "seed " + seed + ": only " + inv.leaderElections() + " elections";
        }
        SimCluster.Host leader = cluster.leader();
        if (leader == null) {
            return "seed " + seed + ": no leader at the end";
        }
        long commit = leader.raft().commitIndex();
        for (SimCluster.Host h : cluster.upHosts()) {
            RaftNode n = h.raft();
            if (n.lastApplied() != commit || n.log().prefixHash(commit) != leader.raft().log().prefixHash(commit)) {
                return "seed " + seed + ": n" + n.id() + " applied a different sequence";
            }
            if (!h.server().stateMachine().snapshotData().equals(leader.server().stateMachine().snapshotData())) {
                return "seed " + seed + ": n" + n.id() + " holds different data";
            }
        }
        LinearizabilityChecker.Result r = new LinearizabilityChecker().check(history.operations());
        if (!r.linearizable()) {
            return "seed " + seed + ": " + r.explanation();
        }
        return null;
    }

    @Test
    void sameCommandsInSameOrderThroughLeaderChanges() {
        List<String> failures = LongStream.rangeClosed(1, 300).parallel()
                .mapToObj(ReplicationTest::run).filter(s -> s != null).toList();
        assertTrue(failures.isEmpty(), () -> String.join("\n", failures.subList(0, Math.min(5, failures.size()))));
    }
}
