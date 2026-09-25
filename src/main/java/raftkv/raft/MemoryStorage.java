package raftkv.raft;

import java.util.ArrayList;
import java.util.List;

/** Storage for the simulator. It outlives node crashes, like a disk. */
public final class MemoryStorage implements Storage {
    private long term;
    private int votedFor;
    private final ArrayList<LogEntry> log = new ArrayList<>();

    @Override
    public State load() {
        return new State(term, votedFor, List.copyOf(log));
    }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public void append(long firstIndex, List<LogEntry> entries) {
        if (firstIndex != log.size() + 1) {
            throw new IllegalStateException("append at " + firstIndex + " but log has " + log.size());
        }
        log.addAll(entries);
    }

    @Override
    public void truncateFrom(long index) {
        while (log.size() >= index) {
            log.remove(log.size() - 1);
        }
    }
}
