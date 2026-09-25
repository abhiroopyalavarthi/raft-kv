package raftkv.sim;

import raftkv.raft.LogEntry;
import raftkv.raft.RaftLog;
import raftkv.raft.RaftNode;
import raftkv.raft.RaftObserver;
import raftkv.raft.Role;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks the five safety properties from Figure 3 of the Raft paper. Commit and apply events are
 * checked as they happen; the rest are checked after every simulator step.
 *
 * <ol>
 *   <li>Election Safety: at most one leader per term</li>
 *   <li>Leader Append-Only: a leader never overwrites or deletes entries in its own log</li>
 *   <li>Log Matching: same index and term means identical logs up to that index</li>
 *   <li>Leader Completeness: a committed entry is in the log of every leader of a later term</li>
 *   <li>State Machine Safety: no two nodes apply different commands at the same index</li>
 * </ol>
 *
 * Logs are compared using prefix hashes (see {@link RaftLog#prefixHash}), so "entry i matches"
 * means "entries 1..i all match".
 */
public final class InvariantChecker implements RaftObserver {
    private final SimCluster cluster;
    private final List<String> violations = new ArrayList<>();

    private final Map<Long, Integer> leaderOfTerm = new HashMap<>();
    private final long[] leaderTerm;
    private final long[] leaderTruncations;

    // committed[i] = prefix hash of entries 1..i; committedTerm[i] = lowest term of a node that reported it committed
    private long[] committedHash = new long[256];
    private long[] committedInTerm = new long[256];
    private long committedMax;

    private long[] appliedHash = new long[256];
    private long appliedMax;

    private long leaderElections;
    private long maxTerm;

    public InvariantChecker(SimCluster cluster) {
        this.cluster = cluster;
        this.leaderTerm = new long[cluster.size() + 1];
        this.leaderTruncations = new long[cluster.size() + 1];
    }

    public boolean failed() {
        return !violations.isEmpty();
    }

    public List<String> violations() {
        return violations;
    }

    public long leaderElections() {
        return leaderElections;
    }

    public long maxTerm() {
        return maxTerm;
    }

    public long committedMax() {
        return committedMax;
    }

    private void violation(String rule, String detail) {
        if (violations.size() < 10) {
            violations.add(rule + " violated at t=" + cluster.sim().now() + ": " + detail);
        }
    }

    // ------------------------------------------------------------ event hooks

    @Override
    public void onRoleChange(RaftNode node, Role from, Role to) {
        maxTerm = Math.max(maxTerm, node.currentTerm());
        if (to != Role.LEADER) {
            return;
        }
        leaderElections++;
        long term = node.currentTerm();
        Integer existing = leaderOfTerm.putIfAbsent(term, node.id());
        if (existing != null && existing != node.id()) {
            violation("Election Safety", "n" + existing + " and n" + node.id() + " both leader in term " + term);
        }
        leaderTerm[node.id()] = term;
        leaderTruncations[node.id()] = node.log().truncations();

        // Leader Completeness: find the highest index known committed by a term before this one.
        long m = committedMax;
        while (m > 0 && committedInTerm[(int) m] >= term) {
            m--;
        }
        if (m > 0) {
            RaftLog log = node.log();
            if (log.lastIndex() < m || log.prefixHash(m) != committedHash[(int) m]) {
                violation("Leader Completeness", "n" + node.id() + " became leader of term " + term
                        + " without committed entries up to index " + m + " (its lastIndex=" + log.lastIndex() + ")");
            }
        }
    }

    @Override
    public void onCommit(RaftNode node, long oldCommit, long newCommit) {
        RaftLog log = node.log();
        long term = node.currentTerm();
        ensureCommitCapacity(newCommit);
        for (long i = oldCommit + 1; i <= newCommit; i++) {
            long h = log.prefixHash(i);
            if (i <= committedMax) {
                if (committedHash[(int) i] != h) {
                    violation("State Machine Safety", "n" + node.id() + " committed a different entry at index " + i);
                }
                committedInTerm[(int) i] = Math.min(committedInTerm[(int) i], term);
            } else {
                committedHash[(int) i] = h;
                committedInTerm[(int) i] = term;
                committedMax = i;
            }
        }
        // Leaders of later terms must already hold what was just committed.
        for (SimCluster.Host other : cluster.upHosts()) {
            RaftNode o = other.raft();
            if (o != node && o.isLeader() && o.currentTerm() > committedInTerm[(int) newCommit]) {
                RaftLog ol = o.log();
                if (ol.lastIndex() < newCommit || ol.prefixHash(newCommit) != committedHash[(int) newCommit]) {
                    violation("Leader Completeness", "entry " + newCommit + " committed in term "
                            + committedInTerm[(int) newCommit] + " is missing from n" + o.id()
                            + ", leader of term " + o.currentTerm());
                }
            }
        }
    }

    @Override
    public void onApply(RaftNode node, long index, LogEntry entry) {
        long h = node.log().prefixHash(index);
        if (index >= appliedHash.length) {
            appliedHash = Arrays.copyOf(appliedHash, (int) Math.max(index + 1, appliedHash.length * 2L));
        }
        if (index <= appliedMax) {
            if (appliedHash[(int) index] != h) {
                violation("State Machine Safety", "n" + node.id() + " applied a different command at index " + index);
            }
        } else {
            if (index != appliedMax + 1) {
                violation("State Machine Safety", "n" + node.id() + " applied index " + index + " out of order");
            }
            appliedHash[(int) index] = h;
            appliedMax = index;
        }
    }

    // ------------------------------------------------------------ per-step checks

    public void afterStep() {
        List<SimCluster.Host> up = cluster.upHosts();
        for (SimCluster.Host h : up) {
            RaftNode n = h.raft();
            if (n.log() == null) {
                continue;
            }
            // Leader Append-Only
            if (n.isLeader() && leaderTerm[n.id()] == n.currentTerm()
                    && n.log().truncations() != leaderTruncations[n.id()]) {
                violation("Leader Append-Only", "n" + n.id() + " truncated its own log while leader of term " + n.currentTerm());
            }
            // Log Matching, only over the part of this log that changed since last step
            long from = n.log().dirtyFrom();
            if (from != Long.MAX_VALUE) {
                checkLogMatching(n, from, up);
                n.log().clearDirty();
            }
            if (n.commitIndex() > n.log().lastIndex()) {
                violation("Commit", "n" + n.id() + " commitIndex " + n.commitIndex() + " beyond its log");
            }
        }
    }

    private void checkLogMatching(RaftNode n, long from, List<SimCluster.Host> up) {
        RaftLog a = n.log();
        for (SimCluster.Host other : up) {
            RaftNode o = other.raft();
            if (o == n || o.log() == null) {
                continue;
            }
            RaftLog b = o.log();
            long end = Math.min(a.lastIndex(), b.lastIndex());
            for (long i = from; i <= end; i++) {
                if (a.term(i) == b.term(i) && a.prefixHash(i) != b.prefixHash(i)) {
                    violation("Log Matching", "n" + n.id() + " and n" + o.id() + " both have term " + a.term(i)
                            + " at index " + i + " but their logs differ before it");
                    return;
                }
            }
        }
    }

    private void ensureCommitCapacity(long index) {
        if (index >= committedHash.length) {
            int size = (int) Math.max(index + 1, committedHash.length * 2L);
            committedHash = Arrays.copyOf(committedHash, size);
            committedInTerm = Arrays.copyOf(committedInTerm, size);
        }
    }
}
