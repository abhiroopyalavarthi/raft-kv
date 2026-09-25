package raftkv.transport;

import raftkv.kv.Op;
import raftkv.raft.LogEntry;

import java.util.List;

/**
 * Every message in the system. Field names follow Figure 2 of the Raft paper.
 * Node ids are 1..N; client endpoints use larger ids.
 */
public sealed interface Message {
    int from();

    int to();

    record RequestVote(int from, int to, long term, long lastLogIndex, long lastLogTerm) implements Message {}

    record RequestVoteReply(int from, int to, long term, boolean voteGranted) implements Message {}

    /**
     * @param seq leader's heartbeat round, echoed in the reply; used to confirm leadership for reads
     */
    record AppendEntries(int from, int to, long term, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit, long seq) implements Message {}

    /**
     * @param matchIndex    on success, the index of the last entry the follower now matches
     * @param conflictIndex on failure, where the leader should retry from
     * @param conflictTerm  on failure, the follower's term at prevLogIndex (0 if its log is too short)
     */
    record AppendEntriesReply(int from, int to, long term, boolean success, long matchIndex,
                              long conflictIndex, long conflictTerm, long seq) implements Message {}

    record ClientRequest(int from, int to, long clientId, long seq, Op op, String key, String value) implements Message {
        public ClientRequest withEndpoints(int newFrom, int newTo) {
            return new ClientRequest(newFrom, newTo, clientId, seq, op, key, value);
        }
    }

    record ClientReply(int from, int to, long clientId, long seq, Status status, String value, int leaderHint) implements Message {}

    enum Status { OK, NOT_LEADER }
}
