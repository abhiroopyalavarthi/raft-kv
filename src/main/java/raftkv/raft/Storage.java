package raftkv.raft;

import java.util.List;

/**
 * Durable Raft state: current term, vote and log. Every method returns only after the
 * data is on stable storage, so a node can safely reply once the call returns.
 */
public interface Storage {
    record State(long term, int votedFor, List<LogEntry> log) {}

    State load();

    void saveTermAndVote(long term, int votedFor);

    /** Append entries so that the first one lands at firstIndex (1-based). */
    void append(long firstIndex, List<LogEntry> entries);

    /** Delete the entry at index and everything after it. */
    void truncateFrom(long index);

    default void close() {}
}
