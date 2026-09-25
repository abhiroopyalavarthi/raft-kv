package raftkv.kv;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import raftkv.client.ClusterSpec;
import raftkv.client.KvClient;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftEnv;
import raftkv.raft.Role;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Milestone 7 in one JVM: 5 nodes over real TCP and real files. Kill the leader mid-write; nothing is lost. */
class RealClusterTest {
    private final Map<Integer, KvNode> nodes = new HashMap<>();

    @AfterEach
    void stopAll() {
        nodes.values().forEach(KvNode::close);
    }

    private KvNode start(int id, Map<Integer, InetSocketAddress> cluster, Path dir) throws Exception {
        KvNode n = new KvNode(id, cluster, dir.resolve("node-" + id), RaftConfig.defaults(), RaftEnv.LOG_OFF, s -> {});
        n.start();
        nodes.put(id, n);
        return n;
    }

    private int leader() throws InterruptedException {
        for (int attempt = 0; attempt < 200; attempt++) {
            for (KvNode n : nodes.values()) {
                if (n.query(s -> s.raft().isLeader())) {
                    return n.id();
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no leader");
    }

    @Test
    void leaderCrashLosesNoAcknowledgedWrite() throws Exception {
        Path dir = Files.createTempDirectory("raft-tcp");
        Map<Integer, InetSocketAddress> cluster = ClusterSpec.local(5, 7401);
        for (int i = 1; i <= 5; i++) {
            start(i, cluster, dir);
        }
        int first = leader();
        try (KvClient client = new KvClient(cluster).withTimeouts(500, 20_000)) {
            for (int i = 0; i < 300; i++) {
                client.put("k" + i, "v" + i);
            }
            nodes.remove(first).close(); // crash: stop the process's threads, keep only what is on disk
            for (int i = 300; i < 600; i++) {
                client.put("k" + i, "v" + i);
            }
            int second = leader();
            assertTrue(second != first);

            KvNode restarted = start(first, cluster, dir);
            long target = nodes.get(second).query(s -> s.raft().commitIndex());
            for (int i = 0; i < 250 && restarted.query(s -> s.raft().lastApplied()) < target; i++) {
                Thread.sleep(20);
            }
            assertTrue(restarted.query(s -> s.raft().lastApplied()) >= target, "restarted node caught up");
            assertEquals(Role.FOLLOWER, restarted.query(s -> s.raft().role()));
            assertEquals("v599", restarted.query(s -> s.stateMachine().get("k599")));

            for (int i = 0; i < 600; i++) {
                assertEquals("v" + i, client.get("k" + i), "acknowledged write k" + i);
            }
        }
    }
}
