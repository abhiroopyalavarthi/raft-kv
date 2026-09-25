package raftkv.raft;

import java.util.EnumSet;
import java.util.Set;

/**
 * @param batching when true, proposals that arrive together are appended with one fsync and
 *                 shipped in one AppendEntries; when false, every proposal gets its own fsync and RPC
 * @param checkQuorum leader steps down if it has not heard from a majority within an election timeout
 */
public record RaftConfig(int electionTimeoutMinMs,
                         int electionTimeoutMaxMs,
                         int heartbeatIntervalMs,
                         int maxEntriesPerAppend,
                         boolean batching,
                         boolean checkQuorum,
                         Set<PlantedBug> bugs) {

    public RaftConfig {
        bugs = bugs.isEmpty() ? EnumSet.noneOf(PlantedBug.class) : EnumSet.copyOf(bugs);
    }

    public static RaftConfig defaults() {
        return new RaftConfig(150, 300, 50, 512, true, true, EnumSet.noneOf(PlantedBug.class));
    }

    public RaftConfig withBatching(boolean on) {
        return new RaftConfig(electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, maxEntriesPerAppend, on, checkQuorum, bugs);
    }

    public RaftConfig withBugs(Set<PlantedBug> b) {
        return new RaftConfig(electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, maxEntriesPerAppend, batching, checkQuorum, b);
    }

    public RaftConfig withCheckQuorum(boolean on) {
        return new RaftConfig(electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, maxEntriesPerAppend, batching, on, bugs);
    }

    public boolean has(PlantedBug bug) {
        return bugs.contains(bug);
    }
}
