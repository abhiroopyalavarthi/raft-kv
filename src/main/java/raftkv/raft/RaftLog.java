package raftkv.raft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The Raft log, 1-indexed, backed by durable {@link Storage}. Index 0 is a sentinel with term 0.
 *
 * <p>Each index also carries a running hash of the whole prefix up to it. Two logs with equal
 * prefix hashes at index i hold identical entries 1..i, which lets the simulator check the
 * Log Matching rule without comparing full logs after every step.
 */
public final class RaftLog {
    private final Storage storage;
    private final ArrayList<LogEntry> entries = new ArrayList<>();
    private long[] prefixHash = new long[64];
    private long truncations;
    private long dirtyFrom = Long.MAX_VALUE;

    public RaftLog(Storage storage, List<LogEntry> initial) {
        this.storage = storage;
        for (LogEntry e : initial) {
            addInMemory(e);
        }
        if (!initial.isEmpty()) {
            dirtyFrom = 1;
        }
    }

    public long lastIndex() {
        return entries.size();
    }

    public long lastTerm() {
        return term(lastIndex());
    }

    public long term(long index) {
        if (index == 0) {
            return 0;
        }
        return get(index).term();
    }

    public LogEntry get(long index) {
        if (index < 1 || index > entries.size()) {
            throw new IndexOutOfBoundsException("log index " + index + ", lastIndex " + entries.size());
        }
        return entries.get((int) (index - 1));
    }

    /** Up to max entries starting at from (empty if from is past the end). */
    public List<LogEntry> slice(long from, int max) {
        if (from > lastIndex()) {
            return List.of();
        }
        int start = (int) (from - 1);
        int end = (int) Math.min(entries.size(), (long) start + max);
        return List.copyOf(entries.subList(start, end));
    }

    /** Hash of entries 1..index (0 for the empty prefix). */
    public long prefixHash(long index) {
        return prefixHash[(int) index];
    }

    /** First index of the run of entries that share the term at index. */
    public long firstIndexOfTerm(long index) {
        long t = term(index);
        while (index > 1 && term(index - 1) == t) {
            index--;
        }
        return index;
    }

    /** Last index holding an entry of term t, or 0 if the log has none. */
    public long lastIndexOfTerm(long t) {
        for (long i = lastIndex(); i >= 1; i--) {
            long ti = term(i);
            if (ti == t) {
                return i;
            }
            if (ti < t) {
                return 0;
            }
        }
        return 0;
    }

    void append(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) {
            return;
        }
        long first = lastIndex() + 1;
        storage.append(first, newEntries);
        for (LogEntry e : newEntries) {
            addInMemory(e);
        }
        dirtyFrom = Math.min(dirtyFrom, first);
    }

    void truncateFrom(long index) {
        if (index > lastIndex()) {
            return;
        }
        storage.truncateFrom(index);
        while (entries.size() >= index) {
            entries.remove(entries.size() - 1);
        }
        truncations++;
        dirtyFrom = Math.min(dirtyFrom, index);
    }

    /** Number of truncations so far; a leader's count must never change during its term. */
    public long truncations() {
        return truncations;
    }

    /** Lowest index changed since the last {@link #clearDirty()}, or Long.MAX_VALUE. */
    public long dirtyFrom() {
        return dirtyFrom;
    }

    public void clearDirty() {
        dirtyFrom = Long.MAX_VALUE;
    }

    private void addInMemory(LogEntry e) {
        entries.add(e);
        int i = entries.size();
        if (i >= prefixHash.length) {
            prefixHash = Arrays.copyOf(prefixHash, prefixHash.length * 2);
        }
        prefixHash[i] = chain(prefixHash[i - 1], e);
    }

    static long chain(long previous, LogEntry e) {
        long h = mix(previous ^ (0x9E3779B97F4A7C15L * (e.term() + 1)));
        for (byte b : e.command()) {
            h = h * 31 + b;
        }
        return mix(h ^ e.command().length);
    }

    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
