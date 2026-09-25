package raftkv.raft;

import org.junit.jupiter.api.Test;
import raftkv.transport.Message.AppendEntries;
import raftkv.transport.Message.AppendEntriesReply;
import raftkv.transport.Message.RequestVote;
import raftkv.transport.Message.RequestVoteReply;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Edge cases of each RPC handler, driven by hand on a single node. */
class RaftNodeTest {
    private static final int[] FIVE = {1, 2, 3, 4, 5};

    private final TestEnv env = new TestEnv();
    private final MemoryStorage storage = new MemoryStorage();
    private final List<Long> applied = new ArrayList<>();

    private RaftNode node(RaftConfig cfg, long term, long... logTerms) {
        storage.saveTermAndVote(term, 0);
        List<LogEntry> entries = new ArrayList<>();
        for (long t : logTerms) {
            entries.add(new LogEntry(t, new byte[]{(byte) t}));
        }
        storage.append(1, entries);
        RaftNode n = new RaftNode(1, FIVE, cfg, env, storage, (index, cmd) -> {
            applied.add(index);
            return null;
        });
        n.start();
        return n;
    }

    private RaftNode node(long term, long... logTerms) {
        return node(RaftConfig.defaults(), term, logTerms);
    }

    private static LogEntry entry(long term) {
        return new LogEntry(term, new byte[]{(byte) term});
    }

    /** Lets the election timer fire and hands the node votes from n2 and n3. */
    private void electLeader(RaftNode n) {
        env.advance(301);
        assertEquals(Role.CANDIDATE, n.role());
        n.onMessage(new RequestVoteReply(2, 1, n.currentTerm(), true));
        n.onMessage(new RequestVoteReply(3, 1, n.currentTerm(), true));
        assertEquals(Role.LEADER, n.role());
    }

    // ------------------------------------------------------------ RequestVote

    @Test
    void rejectsVoteRequestFromOlderTerm() {
        RaftNode n = node(5);
        n.onMessage(new RequestVote(2, 1, 4, 0, 0));
        RequestVoteReply r = env.last(RequestVoteReply.class);
        assertFalse(r.voteGranted());
        assertEquals(5, r.term());
    }

    @Test
    void grantsAtMostOneVotePerTerm() {
        RaftNode n = node(0);
        n.onMessage(new RequestVote(2, 1, 1, 0, 0));
        assertTrue(env.last(RequestVoteReply.class).voteGranted());
        n.onMessage(new RequestVote(3, 1, 1, 0, 0));
        assertFalse(env.last(RequestVoteReply.class).voteGranted(), "second candidate in the same term");
        n.onMessage(new RequestVote(2, 1, 1, 0, 0));
        assertTrue(env.last(RequestVoteReply.class).voteGranted(), "a retried request from the same candidate");
    }

    @Test
    void voteIsOnDiskBeforeTheReply() {
        RaftNode n = node(0);
        n.onMessage(new RequestVote(4, 1, 7, 0, 0));
        assertTrue(env.last(RequestVoteReply.class).voteGranted());
        Storage.State s = storage.load();
        assertEquals(7, s.term());
        assertEquals(4, s.votedFor());
    }

    @Test
    void deniesVoteToCandidateWithLessUpToDateLog() {
        RaftNode n = node(2, 1, 2);
        n.onMessage(new RequestVote(2, 1, 3, 5, 1));
        assertFalse(env.last(RequestVoteReply.class).voteGranted(), "older last term, longer log");
        n.onMessage(new RequestVote(3, 1, 3, 1, 2));
        assertFalse(env.last(RequestVoteReply.class).voteGranted(), "same last term, shorter log");
        n.onMessage(new RequestVote(4, 1, 3, 2, 2));
        assertTrue(env.last(RequestVoteReply.class).voteGranted(), "same last term, same length");
    }

    @Test
    void higherTermInVoteRequestUpdatesTermEvenWhenVoteDenied() {
        RaftNode n = node(2, 1, 2);
        n.onMessage(new RequestVote(2, 1, 9, 0, 0));
        assertFalse(env.last(RequestVoteReply.class).voteGranted());
        assertEquals(9, n.currentTerm());
        assertEquals(0, n.votedFor());
    }

    // ------------------------------------------------------------ AppendEntries (follower)

    @Test
    void rejectsAppendEntriesWhosePreviousEntryDoesNotMatch() {
        RaftNode n = node(2, 1, 1, 2);
        n.onMessage(new AppendEntries(2, 1, 3, 3, 3, List.of(entry(3)), 0, 1));
        AppendEntriesReply r = env.last(AppendEntriesReply.class);
        assertFalse(r.success());
        assertEquals(2, r.conflictTerm(), "follower reports its term at prevLogIndex");
        assertEquals(3, r.conflictIndex(), "and the first index of that term");
        assertEquals(3, n.log().lastIndex(), "nothing changes on a failed check");
    }

    @Test
    void rejectsAppendEntriesPastTheEndOfTheLog() {
        RaftNode n = node(1, 1);
        n.onMessage(new AppendEntries(2, 1, 1, 5, 1, List.of(), 0, 1));
        AppendEntriesReply r = env.last(AppendEntriesReply.class);
        assertFalse(r.success());
        assertEquals(0, r.conflictTerm());
        assertEquals(2, r.conflictIndex());
    }

    @Test
    void rejectsAppendEntriesFromOlderTerm() {
        RaftNode n = node(5, 1);
        n.onMessage(new AppendEntries(2, 1, 4, 1, 1, List.of(entry(4)), 1, 1));
        AppendEntriesReply r = env.last(AppendEntriesReply.class);
        assertFalse(r.success());
        assertEquals(5, r.term());
        assertEquals(1, n.log().lastIndex());
    }

    @Test
    void truncatesConflictingSuffixAndAppends() {
        RaftNode n = node(2, 1, 2, 2);
        n.onMessage(new AppendEntries(2, 1, 3, 1, 1, List.of(entry(3)), 0, 1));
        AppendEntriesReply r = env.last(AppendEntriesReply.class);
        assertTrue(r.success());
        assertEquals(2, r.matchIndex());
        assertEquals(2, n.log().lastIndex());
        assertEquals(3, n.log().term(2));
        assertEquals(2, storage.load().log().size(), "truncation reached the disk");
    }

    @Test
    void staleOrDuplicatedAppendEntriesNeverTruncates() {
        RaftNode n = node(1, 1, 1, 1);
        n.onMessage(new AppendEntries(2, 1, 1, 0, 0, List.of(entry(1)), 0, 1));
        assertTrue(env.last(AppendEntriesReply.class).success());
        assertEquals(3, n.log().lastIndex(), "entries after the matching ones stay");
    }

    @Test
    void followerCommitIsBoundedByWhatTheLeaderSent() {
        RaftNode n = node(1, 1, 1, 1);
        // The leader says commit=10, but only vouched for entries up to index 1.
        n.onMessage(new AppendEntries(2, 1, 1, 0, 0, List.of(entry(1)), 10, 1));
        assertEquals(1, n.commitIndex());
        assertEquals(List.of(1L), applied);
    }

    @Test
    void candidateStepsDownWhenAnotherLeaderAppearsInSameTerm() {
        RaftNode n = node(0);
        env.advance(301);
        assertEquals(Role.CANDIDATE, n.role());
        n.onMessage(new AppendEntries(3, 1, n.currentTerm(), 0, 0, List.of(), 0, 1));
        assertEquals(Role.FOLLOWER, n.role());
        assertEquals(3, n.leaderId());
    }

    // ------------------------------------------------------------ leader behaviour

    @Test
    void newLeaderAppendsNoopAndSendsAppendEntries() {
        RaftNode n = node(0);
        electLeader(n);
        assertEquals(1, n.log().lastIndex());
        assertTrue(n.log().get(1).isNoop());
        assertEquals(4, env.sent(AppendEntries.class).size());
    }

    @Test
    void leaderStepsDownWhenReplyCarriesHigherTerm() {
        RaftNode n = node(0);
        electLeader(n);
        n.onMessage(new AppendEntriesReply(2, 1, 9, false, 0, 0, 0, 1));
        assertEquals(Role.FOLLOWER, n.role());
        assertEquals(9, n.currentTerm());
        assertEquals(0, n.votedFor());
    }

    @Test
    void leaderStepsDownOnAppendEntriesFromNewerTerm() {
        RaftNode n = node(0);
        electLeader(n);
        n.onMessage(new AppendEntries(4, 1, n.currentTerm() + 1, 0, 0, List.of(), 0, 1));
        assertEquals(Role.FOLLOWER, n.role());
        assertEquals(4, n.leaderId());
    }

    @Test
    void leaderBacksUpNextIndexUsingConflictHint() {
        RaftNode n = node(3, 1, 1, 2, 2);
        electLeader(n); // term 4, log [1,1,2,2,noop(4)]
        // n2's log is [1,1,3]: conflict at index 4 with term 3, which the leader doesn't have
        n.onMessage(new AppendEntriesReply(2, 1, n.currentTerm(), false, 0, 3, 3, 1));
        AppendEntries retry = env.last(AppendEntries.class);
        assertEquals(2, retry.to());
        assertEquals(2, retry.prevLogIndex(), "retries from the first index of the conflicting term");
    }

    @Test
    void commitRequiresMajority() {
        RaftNode n = node(0);
        electLeader(n);
        long t = n.currentTerm();
        n.onMessage(new AppendEntriesReply(2, 1, t, true, 1, 0, 0, 1));
        assertEquals(0, n.commitIndex(), "leader + 1 follower is 2 of 5");
        n.onMessage(new AppendEntriesReply(3, 1, t, true, 1, 0, 0, 1));
        assertEquals(1, n.commitIndex(), "3 of 5");
    }

    /**
     * Figure 8 of the Raft paper: a leader must not commit an entry from an earlier term just
     * because a majority now stores it. That entry can still be overwritten by a node whose log
     * ends in a newer term.
     */
    @Test
    void figure8LeaderDoesNotCommitOldTermEntryByCountingReplicas() {
        RaftNode n = node(3, 1, 2); // index 2 was written in term 2; a term-3 leader existed elsewhere
        electLeader(n);             // term 4, log [1, 2, noop(4)]
        long t = n.currentTerm();
        // n2 and n3 hold index 2 but not yet the no-op at index 3: 3 of 5 nodes store index 2.
        n.onMessage(new AppendEntriesReply(2, 1, t, true, 2, 0, 0, 1));
        n.onMessage(new AppendEntriesReply(3, 1, t, true, 2, 0, 0, 1));
        assertEquals(0, n.commitIndex(), "index 2 is from term 2 and must not be committed by counting");
        // Once an entry from the current term is on a majority, everything before it commits too.
        n.onMessage(new AppendEntriesReply(2, 1, t, true, 3, 0, 0, 1));
        n.onMessage(new AppendEntriesReply(3, 1, t, true, 3, 0, 0, 1));
        assertEquals(3, n.commitIndex());
    }

    @Test
    void figure8PlantedBugIsVisibleHere() {
        RaftConfig buggy = RaftConfig.defaults().withBugs(EnumSet.of(PlantedBug.COMMIT_PREVIOUS_TERM_ENTRIES));
        RaftNode n = node(buggy, 3, 1, 2);
        electLeader(n);
        long t = n.currentTerm();
        n.onMessage(new AppendEntriesReply(2, 1, t, true, 2, 0, 0, 1));
        n.onMessage(new AppendEntriesReply(3, 1, t, true, 2, 0, 0, 1));
        assertEquals(2, n.commitIndex(), "the buggy rule commits an old-term entry");
    }

    @Test
    void readWaitsForMajorityToConfirmLeadership() {
        RaftNode n = node(0);
        electLeader(n);
        long t = n.currentTerm();
        n.onMessage(new AppendEntriesReply(2, 1, t, true, 1, 0, 0, 1));
        n.onMessage(new AppendEntriesReply(3, 1, t, true, 1, 0, 0, 1));
        assertEquals(1, n.commitIndex());
        boolean[] ready = {false};
        n.linearizableRead(new RaftCallback() {
            @Override
            public void onSuccess(byte[] r) {
                ready[0] = true;
            }

            @Override
            public void onNotLeader(int hint) {
                throw new AssertionError("still leader");
            }
        });
        assertFalse(ready[0], "must not answer before a fresh round of acks");
        env.advance(0); // the leader sends a new heartbeat round for the read
        long seq = env.last(AppendEntries.class).seq();
        n.onMessage(new AppendEntriesReply(2, 1, t, true, 1, 0, 0, seq));
        assertFalse(ready[0], "leader + 1 is not a majority");
        n.onMessage(new AppendEntriesReply(4, 1, t, true, 1, 0, 0, seq));
        assertTrue(ready[0]);
    }

    @Test
    void leaderWithoutMajorityContactStepsDown() {
        RaftNode n = node(0);
        electLeader(n);
        env.advance(400); // heartbeats go out, nobody answers
        assertEquals(Role.FOLLOWER, n.role());
    }

    @Test
    void proposalsBatchIntoOneAppendPerPeer() {
        RaftNode n = node(0);
        electLeader(n);
        env.sent.clear();
        RaftCallback ignore = new RaftCallback() {
            @Override
            public void onSuccess(byte[] r) {
            }

            @Override
            public void onNotLeader(int h) {
            }
        };
        for (int i = 0; i < 10; i++) {
            n.propose(new byte[]{1, (byte) i}, ignore);
        }
        env.advance(0);
        List<AppendEntries> sent = env.sent(AppendEntries.class);
        assertEquals(4, sent.size(), "one AppendEntries per follower");
        assertEquals(11, n.log().lastIndex());
    }
}
