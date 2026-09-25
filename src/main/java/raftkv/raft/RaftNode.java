package raftkv.raft;

import raftkv.transport.Message;
import raftkv.transport.Message.AppendEntries;
import raftkv.transport.Message.AppendEntriesReply;
import raftkv.transport.Message.RequestVote;
import raftkv.transport.Message.RequestVoteReply;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * One Raft server, written as an event handler: every method runs on the node's single thread,
 * so there are no locks. Follows Figure 2 of the Raft paper, plus three well-known extensions
 * from Ongaro's dissertation:
 * <ul>
 *   <li>a no-op entry at the start of each term, so the leader can commit earlier entries</li>
 *   <li>ReadIndex reads: confirm leadership with a heartbeat round instead of writing to the log</li>
 *   <li>check-quorum: a leader that can't reach a majority steps down</li>
 * </ul>
 */
public final class RaftNode {
    private record PendingProposal(long term, RaftCallback callback) {}

    private static final class PendingRead {
        final RaftCallback callback;
        long readIndex = -1;
        long requiredSeq;

        PendingRead(RaftCallback callback) {
            this.callback = callback;
        }
    }

    private final int id;
    private final int[] peers;
    private final int majority;
    private final RaftConfig cfg;
    private final RaftEnv env;
    private final Storage storage;
    private final StateMachine stateMachine;
    private RaftObserver observer = RaftObserver.NONE;

    // persistent state (written to storage before we reply to anyone)
    private long currentTerm;
    private int votedFor; // 0 = none
    private RaftLog log;

    // volatile state
    private Role role = Role.FOLLOWER;
    private int leaderId;
    private long commitIndex;
    private long lastApplied;
    private boolean running;
    private long roleEpoch;
    private long electionDeadline;
    private boolean electionTimerArmed;

    // candidate state
    private final boolean[] votes;
    private int voteCount;

    // leader state
    private final long[] nextIndex;
    private final long[] matchIndex;
    private final long[] ackedSeq;
    private final long[] lastAckTime;
    private long heartbeatSeq;
    private long leaderSince;
    private final TreeMap<Long, PendingProposal> pending = new TreeMap<>();
    private final ArrayList<PendingRead> reads = new ArrayList<>();
    private boolean readRoundScheduled;
    private final ArrayList<byte[]> bufferedCommands = new ArrayList<>();
    private final ArrayList<RaftCallback> bufferedCallbacks = new ArrayList<>();
    private boolean flushScheduled;

    public RaftNode(int id, int[] members, RaftConfig cfg, RaftEnv env, Storage storage, StateMachine stateMachine) {
        this.id = id;
        this.cfg = cfg;
        this.env = env;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.peers = Arrays.stream(members).filter(m -> m != id).sorted().toArray();
        this.majority = members.length / 2 + 1;
        int maxId = Arrays.stream(members).max().orElse(id);
        this.votes = new boolean[maxId + 1];
        this.nextIndex = new long[maxId + 1];
        this.matchIndex = new long[maxId + 1];
        this.ackedSeq = new long[maxId + 1];
        this.lastAckTime = new long[maxId + 1];
    }

    public void setObserver(RaftObserver observer) {
        this.observer = observer;
    }

    // ------------------------------------------------------------------ lifecycle

    public void start() {
        Storage.State state = storage.load();
        currentTerm = state.term();
        votedFor = state.votedFor();
        log = new RaftLog(storage, state.log());
        running = true;
        info("started: votedFor=%d lastIndex=%d lastTerm=%d", votedFor, log.lastIndex(), log.lastTerm());
        resetElectionTimer();
        armElectionTimer();
    }

    /** Stop reacting to anything (used when a node is killed). */
    public void stop() {
        running = false;
    }

    // ------------------------------------------------------------------ inbound messages

    public void onMessage(Message m) {
        if (!running) {
            return;
        }
        switch (m) {
            case RequestVote rv -> handleRequestVote(rv);
            case RequestVoteReply r -> handleVoteReply(r);
            case AppendEntries ae -> handleAppendEntries(ae);
            case AppendEntriesReply r -> handleAppendReply(r);
            default -> throw new IllegalArgumentException("not a Raft message: " + m);
        }
    }

    private void handleRequestVote(RequestVote rv) {
        if (rv.term() > currentTerm) {
            observeHigherTerm(rv.term());
        }
        boolean granted = false;
        if (rv.term() == currentTerm
                && (votedFor == 0 || votedFor == rv.from())
                && candidateLogIsUpToDate(rv.lastLogTerm(), rv.lastLogIndex())) {
            granted = true;
            if (votedFor != rv.from()) {
                votedFor = rv.from();
                persistTermAndVote();
                info("voted for n%d", rv.from());
            }
            resetElectionTimer();
        }
        env.send(new RequestVoteReply(id, rv.from(), currentTerm, granted));
    }

    private boolean candidateLogIsUpToDate(long lastLogTerm, long lastLogIndex) {
        long myTerm = log.lastTerm();
        return lastLogTerm > myTerm || (lastLogTerm == myTerm && lastLogIndex >= log.lastIndex());
    }

    private void handleVoteReply(RequestVoteReply r) {
        if (r.term() > currentTerm) {
            observeHigherTerm(r.term());
            return;
        }
        if (role != Role.CANDIDATE || r.term() != currentTerm || !r.voteGranted()) {
            return;
        }
        if (!votes[r.from()]) {
            votes[r.from()] = true;
            voteCount++;
        }
        if (voteCount >= majority) {
            becomeLeader();
        }
    }

    private void handleAppendEntries(AppendEntries ae) {
        if (ae.term() > currentTerm) {
            observeHigherTerm(ae.term());
        }
        if (ae.term() < currentTerm) {
            env.send(new AppendEntriesReply(id, ae.from(), currentTerm, false, 0, 0, 0, ae.seq()));
            return;
        }
        if (role == Role.LEADER) {
            throw new IllegalStateException("n" + id + " is leader of term " + currentTerm
                    + " but received AppendEntries from n" + ae.from() + " for the same term");
        }
        if (role == Role.CANDIDATE) {
            setRole(Role.FOLLOWER);
        }
        if (leaderId != ae.from()) {
            leaderId = ae.from();
            info("following leader n%d", leaderId);
        }
        resetElectionTimer();

        long prev = ae.prevLogIndex();
        if (prev > log.lastIndex()) {
            env.send(new AppendEntriesReply(id, ae.from(), currentTerm, false, 0, log.lastIndex() + 1, 0, ae.seq()));
            return;
        }
        if (log.term(prev) != ae.prevLogTerm()) {
            long conflictTerm = log.term(prev);
            long conflictIndex = log.firstIndexOfTerm(prev);
            env.send(new AppendEntriesReply(id, ae.from(), currentTerm, false, 0, conflictIndex, conflictTerm, ae.seq()));
            return;
        }

        // Skip entries we already have; truncate only at a real conflict (messages can be stale or duplicated).
        List<LogEntry> entries = ae.entries();
        int i = 0;
        long index = prev + 1;
        while (i < entries.size() && index <= log.lastIndex()) {
            if (log.term(index) != entries.get(i).term()) {
                if (index <= commitIndex) {
                    throw new IllegalStateException("n" + id + " asked to overwrite committed entry " + index);
                }
                debug("conflict at %d (have term %d, leader has %d): truncating", index, log.term(index), entries.get(i).term());
                log.truncateFrom(index);
                break;
            }
            i++;
            index++;
        }
        if (i < entries.size()) {
            log.append(entries.subList(i, entries.size()));
        }

        long lastNew = prev + entries.size();
        if (ae.leaderCommit() > commitIndex) {
            long newCommit = Math.min(ae.leaderCommit(), lastNew);
            if (newCommit > commitIndex) {
                setCommitIndex(newCommit);
            }
        }
        env.send(new AppendEntriesReply(id, ae.from(), currentTerm, true, lastNew, 0, 0, ae.seq()));
    }

    private void handleAppendReply(AppendEntriesReply r) {
        if (r.term() > currentTerm) {
            observeHigherTerm(r.term());
            return;
        }
        if (role != Role.LEADER || r.term() != currentTerm) {
            return;
        }
        int p = r.from();
        lastAckTime[p] = env.now();
        ackedSeq[p] = Math.max(ackedSeq[p], r.seq());
        if (r.success()) {
            if (r.matchIndex() > matchIndex[p]) {
                matchIndex[p] = r.matchIndex();
                advanceCommitIndex();
            }
            nextIndex[p] = Math.max(nextIndex[p], matchIndex[p] + 1);
            if (nextIndex[p] <= log.lastIndex()) {
                sendAppend(p, cfg.maxEntriesPerAppend());
            }
        } else {
            long next = r.conflictIndex();
            if (r.conflictTerm() > 0) {
                long last = log.lastIndexOfTerm(r.conflictTerm());
                if (last > 0) {
                    next = last + 1;
                }
            }
            next = Math.max(next, matchIndex[p] + 1);
            next = Math.min(next, log.lastIndex() + 1);
            nextIndex[p] = next;
            sendAppend(p, cfg.maxEntriesPerAppend());
        }
        serviceReads();
    }

    // ------------------------------------------------------------------ elections

    private void resetElectionTimer() {
        int span = cfg.electionTimeoutMaxMs() - cfg.electionTimeoutMinMs();
        electionDeadline = env.now() + cfg.electionTimeoutMinMs() + (span > 0 ? env.random().nextInt(span + 1) : 0);
    }

    private void armElectionTimer() {
        if (electionTimerArmed) {
            return;
        }
        electionTimerArmed = true;
        env.schedule(Math.max(0, electionDeadline - env.now()), this::onElectionTimer);
    }

    private void onElectionTimer() {
        electionTimerArmed = false;
        if (!running || role == Role.LEADER) {
            return;
        }
        if (env.now() >= electionDeadline) {
            startElection();
        }
        if (role != Role.LEADER) {
            armElectionTimer();
        }
    }

    private void startElection() {
        currentTerm++;
        votedFor = id;
        persistTermAndVote();
        setRole(Role.CANDIDATE);
        leaderId = 0;
        Arrays.fill(votes, false);
        votes[id] = true;
        voteCount = 1;
        resetElectionTimer();
        info("election timeout: starting election (lastIndex=%d lastTerm=%d)", log.lastIndex(), log.lastTerm());
        if (voteCount >= majority) {
            becomeLeader();
            return;
        }
        for (int p : peers) {
            env.send(new RequestVote(id, p, currentTerm, log.lastIndex(), log.lastTerm()));
        }
    }

    private void becomeLeader() {
        setRole(Role.LEADER);
        leaderId = id;
        leaderSince = env.now();
        long next = log.lastIndex() + 1;
        for (int p : peers) {
            nextIndex[p] = next;
            matchIndex[p] = 0;
            ackedSeq[p] = 0;
            lastAckTime[p] = env.now();
        }
        info("became LEADER with %d votes (lastIndex=%d)", voteCount, log.lastIndex());
        // A no-op in the new term lets us commit entries left over from earlier terms (Raft §5.4.2, §8).
        log.append(List.of(new LogEntry(currentTerm, LogEntry.NOOP)));
        broadcastAppend();
        scheduleHeartbeat(roleEpoch);
        advanceCommitIndex();
    }

    private void observeHigherTerm(long term) {
        info("saw higher term %d", term);
        currentTerm = term;
        votedFor = 0;
        persistTermAndVote();
        leaderId = 0;
        if (role != Role.FOLLOWER) {
            becomeFollower();
        }
    }

    private void becomeFollower() {
        boolean wasLeader = role == Role.LEADER;
        setRole(Role.FOLLOWER);
        if (wasLeader) {
            resetElectionTimer();
        }
        armElectionTimer();
    }

    private void setRole(Role newRole) {
        if (role == newRole) {
            return;
        }
        Role old = role;
        role = newRole;
        roleEpoch++;
        if (old == Role.LEADER) {
            failLeaderWork();
        }
        debug("role %s -> %s", old, newRole);
        observer.onRoleChange(this, old, newRole);
    }

    /** Callers waiting on this leader get NOT_LEADER; clients retry (dedup makes that safe). */
    private void failLeaderWork() {
        List<RaftCallback> toFail = new ArrayList<>();
        for (PendingProposal p : pending.values()) {
            toFail.add(p.callback());
        }
        pending.clear();
        for (PendingRead r : reads) {
            toFail.add(r.callback);
        }
        reads.clear();
        toFail.addAll(bufferedCallbacks);
        bufferedCallbacks.clear();
        bufferedCommands.clear();
        for (RaftCallback cb : toFail) {
            cb.onNotLeader(0);
        }
    }

    // ------------------------------------------------------------------ replication (leader)

    private void scheduleHeartbeat(long epoch) {
        env.schedule(cfg.heartbeatIntervalMs(), () -> {
            if (!running || role != Role.LEADER || epoch != roleEpoch) {
                return;
            }
            if (cfg.checkQuorum() && !heardFromMajorityRecently()) {
                info("no contact with a majority for %d ms: stepping down", cfg.electionTimeoutMaxMs());
                becomeFollower();
                return;
            }
            broadcastAppend();
            scheduleHeartbeat(epoch);
        });
    }

    private boolean heardFromMajorityRecently() {
        long now = env.now();
        if (now - leaderSince < cfg.electionTimeoutMaxMs()) {
            return true;
        }
        int count = 1;
        for (int p : peers) {
            if (now - lastAckTime[p] <= cfg.electionTimeoutMaxMs()) {
                count++;
            }
        }
        return count >= majority;
    }

    private void broadcastAppend() {
        heartbeatSeq++;
        for (int p : peers) {
            sendAppend(p, cfg.maxEntriesPerAppend());
        }
    }

    /** Sends entries from nextIndex[p]. Advances nextIndex optimistically (pipelining). */
    private void sendAppend(int p, int limit) {
        long next = nextIndex[p];
        long prev = next - 1;
        List<LogEntry> entries = log.slice(next, limit);
        env.send(new AppendEntries(id, p, currentTerm, prev, log.term(prev), entries, commitIndex, heartbeatSeq));
        if (!entries.isEmpty()) {
            nextIndex[p] = next + entries.size();
        }
    }

    private void advanceCommitIndex() {
        if (role != Role.LEADER) {
            return;
        }
        long[] m = new long[peers.length + 1];
        m[0] = log.lastIndex();
        for (int i = 0; i < peers.length; i++) {
            m[i + 1] = matchIndex[peers[i]];
        }
        Arrays.sort(m);
        long n = m[m.length - majority]; // highest index stored on a majority
        // Only entries from the current term are committed by counting replicas (Raft §5.4.2).
        boolean allowed = log.term(n) == currentTerm || cfg.has(PlantedBug.COMMIT_PREVIOUS_TERM_ENTRIES);
        if (n > commitIndex && allowed) {
            setCommitIndex(n);
        }
    }

    // ------------------------------------------------------------------ commit and apply

    private void setCommitIndex(long newCommit) {
        long old = commitIndex;
        commitIndex = newCommit;
        debug("commitIndex %d -> %d", old, newCommit);
        observer.onCommit(this, old, newCommit);
        applyCommitted();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            long index = ++lastApplied;
            LogEntry e = log.get(index);
            byte[] result = e.isNoop() ? null : stateMachine.apply(index, e.command());
            observer.onApply(this, index, e);
            PendingProposal p = pending.remove(index);
            if (p != null) {
                if (p.term() == e.term()) {
                    p.callback().onSuccess(result);
                } else {
                    p.callback().onNotLeader(leaderId);
                }
            }
        }
        serviceReads();
    }

    // ------------------------------------------------------------------ client-facing API

    /** Replicate a command. The callback fires once it is committed and applied, or if we lose leadership. */
    public void propose(byte[] command, RaftCallback callback) {
        if (!running) {
            return;
        }
        if (role != Role.LEADER) {
            callback.onNotLeader(leaderId);
            return;
        }
        if (cfg.batching()) {
            bufferedCommands.add(command);
            bufferedCallbacks.add(callback);
            if (!flushScheduled) {
                flushScheduled = true;
                env.schedule(0, this::flushProposals);
            }
        } else {
            long index = log.lastIndex() + 1;
            log.append(List.of(new LogEntry(currentTerm, command)));
            pending.put(index, new PendingProposal(currentTerm, callback));
            for (int p : peers) {
                sendAppend(p, 1);
            }
            advanceCommitIndex();
        }
    }

    /** Group commit: everything proposed since the last flush goes in with one fsync and one RPC per peer. */
    private void flushProposals() {
        flushScheduled = false;
        if (!running || bufferedCommands.isEmpty()) {
            return;
        }
        if (role != Role.LEADER) {
            failLeaderWork();
            return;
        }
        long first = log.lastIndex() + 1;
        List<LogEntry> entries = new ArrayList<>(bufferedCommands.size());
        for (byte[] c : bufferedCommands) {
            entries.add(new LogEntry(currentTerm, c));
        }
        log.append(entries);
        for (int i = 0; i < bufferedCallbacks.size(); i++) {
            pending.put(first + i, new PendingProposal(currentTerm, bufferedCallbacks.get(i)));
        }
        bufferedCommands.clear();
        bufferedCallbacks.clear();
        for (int p : peers) {
            sendAppend(p, cfg.maxEntriesPerAppend());
        }
        advanceCommitIndex();
    }

    /**
     * Linearizable read (ReadIndex, dissertation §6.4). The callback fires once it is safe to read the
     * local state machine: this leader has committed an entry in its term, a majority has confirmed it
     * is still leader after the read arrived, and everything up to the read index has been applied.
     */
    public void linearizableRead(RaftCallback callback) {
        if (!running) {
            return;
        }
        if (role != Role.LEADER) {
            callback.onNotLeader(leaderId);
            return;
        }
        if (cfg.has(PlantedBug.READ_WITHOUT_QUORUM_CHECK)) {
            callback.onSuccess(null);
            return;
        }
        reads.add(new PendingRead(callback));
        serviceReads();
    }

    private void serviceReads() {
        if (reads.isEmpty() || role != Role.LEADER) {
            return;
        }
        boolean committedInTerm = log.term(commitIndex) == currentTerm;
        boolean needRound = false;
        for (PendingRead r : reads) {
            if (r.readIndex < 0 && committedInTerm) {
                r.readIndex = commitIndex;
                r.requiredSeq = heartbeatSeq + 1;
                needRound = true;
            }
        }
        if (needRound && !readRoundScheduled) {
            readRoundScheduled = true;
            env.schedule(0, () -> {
                readRoundScheduled = false;
                if (running && role == Role.LEADER) {
                    broadcastAppend();
                }
            });
        }
        List<RaftCallback> ready = null;
        for (int i = 0; i < reads.size(); i++) {
            PendingRead r = reads.get(i);
            if (r.readIndex >= 0 && lastApplied >= r.readIndex && majorityAcked(r.requiredSeq)) {
                if (ready == null) {
                    ready = new ArrayList<>();
                }
                ready.add(r.callback);
                reads.remove(i--);
            }
        }
        if (ready != null) {
            for (RaftCallback cb : ready) {
                cb.onSuccess(null);
            }
        }
    }

    private boolean majorityAcked(long seq) {
        int count = 1;
        for (int p : peers) {
            if (ackedSeq[p] >= seq) {
                count++;
            }
        }
        return count >= majority;
    }

    // ------------------------------------------------------------------ persistence + logging

    private void persistTermAndVote() {
        storage.saveTermAndVote(currentTerm, votedFor);
    }

    private void info(String fmt, Object... args) {
        if (env.logLevel() >= RaftEnv.LOG_INFO) {
            env.log(prefix() + String.format(fmt, args));
        }
    }

    private void debug(String fmt, Object... args) {
        if (env.logLevel() >= RaftEnv.LOG_DEBUG) {
            env.log(prefix() + String.format(fmt, args));
        }
    }

    private String prefix() {
        return "n" + id + " T" + currentTerm + " " + role + " ci=" + commitIndex + " li=" + (log == null ? 0 : log.lastIndex()) + " | ";
    }

    // ------------------------------------------------------------------ read-only accessors

    public int id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public boolean isLeader() {
        return running && role == Role.LEADER;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public int votedFor() {
        return votedFor;
    }

    public int leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public RaftLog log() {
        return log;
    }

    public boolean isRunning() {
        return running;
    }

    public long nextIndexFor(int peer) {
        return nextIndex[peer];
    }

    public long matchIndexFor(int peer) {
        return matchIndex[peer];
    }

    Map<Long, ?> pendingProposals() {
        return pending;
    }

    public String status() {
        return "id=" + id + " role=" + role + " term=" + currentTerm + " leader=" + leaderId
                + " commit=" + commitIndex + " applied=" + lastApplied + " lastIndex=" + log.lastIndex();
    }
}
