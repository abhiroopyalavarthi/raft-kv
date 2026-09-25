package raftkv.sim;

import org.junit.jupiter.api.Test;
import raftkv.checker.History;
import raftkv.kv.Command;
import raftkv.kv.KvStateMachine;
import raftkv.raft.LogEntry;
import raftkv.raft.PlantedBug;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftNode;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Milestone 3: with a lossy network clients retry a lot, and each write still takes effect exactly once. */
class DedupTest {

    record Counts(long logCommands, long distinctCommands, long executed) {}

    static Counts run(long seed, RaftConfig cfg) {
        Simulator sim = new Simulator(seed);
        SimNetwork net = new SimNetwork(sim);
        net.setDelay(1, 20);
        net.setDropRate(0.25);
        SimCluster cluster = new SimCluster(sim, net, 5, cfg);
        History history = new History();
        for (int i = 0; i < 4; i++) {
            SimClient c = new SimClient(100 + i, i + 1, sim, net, history, 5, new String[]{"a", "b"}, 60);
            c.stopIssuingAt(2500);
            c.start(0);
        }
        cluster.startAll();
        sim.runUntil(3000);
        net.setDropRate(0);
        sim.runUntil(4000);
        SimCluster.Host leader = cluster.leader();
        RaftNode n = leader.raft();
        long commands = 0;
        Set<String> distinct = new HashSet<>();
        for (long i = 1; i <= n.lastApplied(); i++) {
            LogEntry e = n.log().get(i);
            if (!e.isNoop()) {
                Command c = Command.decode(e.command());
                commands++;
                distinct.add(c.clientId() + "/" + c.seq());
            }
        }
        KvStateMachine sm = leader.server().stateMachine();
        return new Counts(commands, distinct.size(), sm.executedWrites());
    }

    @Test
    void retriedWritesApplyExactlyOnce() {
        long duplicates = 0;
        for (long seed = 1; seed <= 50; seed++) {
            Counts c = run(seed, RaftConfig.defaults());
            duplicates += c.logCommands() - c.distinctCommands();
            assertEquals(c.distinctCommands(), c.executed(), "seed " + seed + ": each (client, seq) executes once");
        }
        assertTrue(duplicates > 0, "the lossy network did cause duplicate entries in the log");
        System.out.println("dedup: " + duplicates + " duplicate log entries across 50 runs, all ignored");
    }

    @Test
    void withoutDedupDuplicatesExecuteAgain() {
        RaftConfig buggy = RaftConfig.defaults().withBugs(EnumSet.of(PlantedBug.NO_DEDUP));
        long extra = 0;
        for (long seed = 1; seed <= 50; seed++) {
            Counts c = run(seed, buggy);
            extra += c.executed() - c.distinctCommands();
        }
        assertTrue(extra > 0, "the planted bug re-executes retried writes");
    }
}
