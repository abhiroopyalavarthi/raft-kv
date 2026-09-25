package raftkv.sim;

import org.junit.jupiter.api.Test;
import raftkv.raft.RaftConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Milestone 1: across 1,000 seeds one leader emerges, a new one replaces it after a crash, never two per term. */
class ElectionTest {

    record Outcome(long seed, String error, long firstElectionMs, long failoverMs) {}

    static Outcome run(long seed) {
        Simulator sim = new Simulator(seed);
        SimNetwork net = new SimNetwork(sim);
        net.setDelay(1, 10);
        net.setDropRate(0.05);
        SimCluster cluster = new SimCluster(sim, net, 5, RaftConfig.defaults());
        InvariantChecker inv = new InvariantChecker(cluster);
        cluster.setObserver(inv);
        cluster.startAll();

        long first = -1;
        while (sim.now() < 3000 && first < 0) {
            sim.runUntil(sim.now() + 1, inv::afterStep, inv::failed);
            if (cluster.leader() != null) {
                first = sim.now();
            }
        }
        sim.runUntil(3000, inv::afterStep, inv::failed);
        if (inv.failed()) {
            return new Outcome(seed, inv.violations().get(0), first, -1);
        }
        long leaders = cluster.upHosts().stream().filter(h -> h.raft().isLeader()).count();
        if (leaders != 1) {
            return new Outcome(seed, leaders + " leaders after 3 s", first, -1);
        }
        SimCluster.Host old = cluster.leader();
        long oldTerm = old.raft().currentTerm();
        cluster.crash(old.id);
        long crashAt = sim.now();
        long failover = -1;
        while (sim.now() < crashAt + 3000 && failover < 0) {
            sim.runUntil(sim.now() + 1, inv::afterStep, inv::failed);
            if (cluster.leader() != null) {
                failover = sim.now() - crashAt;
            }
        }
        sim.runUntil(crashAt + 3000, inv::afterStep, inv::failed);
        if (inv.failed()) {
            return new Outcome(seed, inv.violations().get(0), first, failover);
        }
        SimCluster.Host next = cluster.leader();
        if (next == null) {
            return new Outcome(seed, "no leader after the old one crashed", first, failover);
        }
        if (next.raft().currentTerm() <= oldTerm || next.id == old.id) {
            return new Outcome(seed, "new leader did not take a higher term", first, failover);
        }
        return new Outcome(seed, null, first, failover);
    }

    @Test
    void oneLeaderEmergesAndIsReplacedAfterCrash() {
        List<Outcome> outcomes = LongStream.rangeClosed(1, 1000).parallel().mapToObj(ElectionTest::run).toList();
        List<Outcome> bad = new ArrayList<>();
        for (Outcome o : outcomes) {
            if (o.error() != null) {
                bad.add(o);
            }
        }
        assertTrue(bad.isEmpty(), () -> "failing seeds: " + bad.subList(0, Math.min(5, bad.size())));
        long[] first = outcomes.stream().mapToLong(Outcome::firstElectionMs).sorted().toArray();
        long[] failover = outcomes.stream().mapToLong(Outcome::failoverMs).sorted().toArray();
        System.out.printf("election: 1000 seeds, first leader p50 %d ms / max %d ms; after leader crash p50 %d ms / p99 %d ms / max %d ms%n",
                first[500], first[999], failover[500], failover[990], failover[999]);
        assertEquals(1000, outcomes.size());
    }
}
