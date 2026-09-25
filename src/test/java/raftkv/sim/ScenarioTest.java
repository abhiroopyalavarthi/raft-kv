package raftkv.sim;

import org.junit.jupiter.api.Test;
import raftkv.checker.History;
import raftkv.checker.LinearizabilityChecker;
import raftkv.kv.Op;
import raftkv.raft.PlantedBug;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftNode;
import raftkv.raft.Role;
import raftkv.transport.Message.ClientReply;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hand-built situations known to break naive Raft implementations. */
class ScenarioTest {

    private SimCluster cluster;
    private InvariantChecker inv;

    private void setUp(long seed, RaftConfig cfg) {
        Simulator sim = new Simulator(seed);
        SimNetwork net = new SimNetwork(sim);
        net.setDelay(1, 3);
        cluster = new SimCluster(sim, net, 5, cfg);
        inv = new InvariantChecker(cluster);
        cluster.setObserver(inv);
        cluster.startAll();
        run(1000);
        assertNotNull(cluster.leader(), "a leader is elected");
    }

    private void run(long ms) {
        Simulator sim = cluster.sim();
        sim.runUntil(sim.now() + ms, inv::afterStep, inv::failed);
        assertFalse(inv.failed(), () -> String.valueOf(inv.violations()));
    }

    private int[] isolate(int node) {
        int[] groups = new int[cluster.size() + 1];
        groups[node] = 1;
        return groups;
    }

    private void assertAllNodesAgree() {
        SimCluster.Host leader = cluster.leader();
        assertNotNull(leader);
        for (SimCluster.Host h : cluster.upHosts()) {
            RaftNode n = h.raft();
            assertEquals(leader.raft().commitIndex(), n.lastApplied(), "n" + n.id() + " applied everything");
            assertEquals(leader.raft().log().prefixHash(n.lastApplied()), n.log().prefixHash(n.lastApplied()),
                    "n" + n.id() + " has the same log");
            assertEquals(leader.server().stateMachine().snapshotData(), h.server().stateMachine().snapshotData());
        }
    }

    /**
     * A leader cut off from everyone still believes it is leader. It must not commit writes, and it
     * must not answer reads from its (now stale) state, while the majority elects a new leader.
     */
    @Test
    void partitionedLeaderCannotCommitOrServeStaleReads() {
        setUp(42, RaftConfig.defaults().withCheckQuorum(false));
        ScriptedClient c = new ScriptedClient(200, 1, cluster, inv);
        int oldLeader = cluster.leader().id;
        assertTrue(ScriptedClient.ok(c.call(oldLeader, Op.PUT, "x", "v1", 200)));

        cluster.network().partition(isolate(oldLeader));
        assertNull(c.call(oldLeader, Op.PUT, "x", "v2", 500), "minority leader cannot commit");
        run(1000);
        SimCluster.Host newLeader = cluster.leader();
        assertTrue(newLeader.id != oldLeader, "majority side elected a new leader");
        assertTrue(ScriptedClient.ok(c.call(newLeader.id, Op.PUT, "x", "v3", 300)));

        assertEquals(Role.LEADER, cluster.host(oldLeader).raft().role(), "old leader still thinks it leads");
        assertNull(c.call(oldLeader, Op.GET, "x", null, 500), "ReadIndex blocks: no majority confirms it");

        cluster.network().heal();
        run(1000);
        assertEquals(Role.FOLLOWER, cluster.host(oldLeader).raft().role());
        assertEquals("v3", cluster.host(oldLeader).server().stateMachine().get("x"), "its uncommitted v2 was discarded");
        ClientReply r = c.call(oldLeader, Op.GET, "x", null, 300);
        assertEquals(newLeader.id, r.leaderHint(), "follower redirects to the leader");
        assertEquals("v3", c.call(newLeader.id, Op.GET, "x", null, 300).value());
        assertAllNodesAgree();
    }

    /** The same situation with the planted bug: the old leader answers the read with stale data. */
    @Test
    void readWithoutQuorumCheckServesStaleData() {
        setUp(42, RaftConfig.defaults().withCheckQuorum(false).withBugs(EnumSet.of(PlantedBug.READ_WITHOUT_QUORUM_CHECK)));
        ScriptedClient c = new ScriptedClient(200, 1, cluster, inv);
        int oldLeader = cluster.leader().id;
        assertTrue(ScriptedClient.ok(c.call(oldLeader, Op.PUT, "x", "v1", 200)));
        cluster.network().partition(isolate(oldLeader));
        run(1000);
        int newLeader = cluster.leader().id;
        assertTrue(ScriptedClient.ok(c.call(newLeader, Op.PUT, "x", "v3", 300)));
        ClientReply stale = c.call(oldLeader, Op.GET, "x", null, 300);
        assertTrue(ScriptedClient.ok(stale));
        assertEquals("v1", stale.value(), "a completed write of v3 is invisible: not linearizable");
    }

    @Test
    void checkQuorumMakesAnIsolatedLeaderStepDown() {
        setUp(3, RaftConfig.defaults());
        int oldLeader = cluster.leader().id;
        cluster.network().partition(isolate(oldLeader));
        run(600);
        // It gives up leadership; afterwards it keeps timing out and starting elections it can't win.
        assertNotEquals(Role.LEADER, cluster.host(oldLeader).raft().role());
    }

    /** A node misses many terms and thousands of entries, then comes back and catches up. */
    @Test
    void nodeOfflineForManyTermsCatchesUp() {
        setUp(7, RaftConfig.defaults());
        ScriptedClient c = new ScriptedClient(200, 1, cluster, inv);
        int absent = cluster.leader().id == 5 ? 4 : 5;
        cluster.crash(absent);
        long termWhenLeft = cluster.host(absent).raft().currentTerm();
        int writes = 0;
        for (int round = 0; round < 8; round++) {
            for (int i = 0; i < 25; i++) {
                SimCluster.Host l = cluster.leader();
                if (l != null && ScriptedClient.ok(c.call(l.id, Op.PUT, "k" + (writes % 50), "v" + writes, 100))) {
                    writes++;
                }
            }
            SimCluster.Host l = cluster.leader();
            assertNotNull(l);
            cluster.crash(l.id);
            run(700);
            cluster.restart(l.id);
            run(200);
        }
        SimCluster.Host leader = cluster.leader();
        assertTrue(leader.raft().currentTerm() >= termWhenLeft + 8, "the cluster moved through many terms");
        assertTrue(writes >= 150, "writes kept succeeding: " + writes);

        cluster.restart(absent);
        run(1500);
        RaftNode back = cluster.host(absent).raft();
        assertEquals(cluster.leader().raft().currentTerm(), back.currentTerm());
        assertAllNodesAgree();
    }

    /** Every node loses power at once. Nothing acknowledged may be lost when they come back. */
    @Test
    void wholeClusterRestartLosesNoCommittedEntry() {
        setUp(11, RaftConfig.defaults());
        Simulator sim = cluster.sim();
        History history = new History();
        String[] keys = {"a", "b", "c"};
        List<SimClient> clients = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            SimClient sc = new SimClient(300 + i, 10 + i, sim, cluster.network(), history, 5, keys, 120);
            sc.stopIssuingAt(sim.now() + 3500);
            sc.start(i);
            clients.add(sc);
        }
        run(1000);
        SimCluster.Host leader = cluster.leader();
        long committed = leader.raft().commitIndex();
        long hash = leader.raft().log().prefixHash(committed);
        assertTrue(committed > 20);
        for (int i = 1; i <= 5; i++) {
            cluster.crash(i);
        }
        run(300);
        for (int i = 1; i <= 5; i++) {
            cluster.restart(i);
        }
        run(3500);
        SimCluster.Host after = cluster.leader();
        assertTrue(after.raft().log().lastIndex() >= committed);
        assertEquals(hash, after.raft().log().prefixHash(committed), "every committed entry survived");
        for (SimClient sc : clients) {
            assertTrue(sc.idle(), "all client operations finished");
        }
        LinearizabilityChecker.Result r = new LinearizabilityChecker().check(history.operations());
        assertTrue(r.linearizable(), r::explanation);
        assertAllNodesAgree();
    }
}
