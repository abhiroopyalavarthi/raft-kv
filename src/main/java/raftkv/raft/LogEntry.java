package raftkv.raft;

/** One slot in the Raft log. An empty command is a no-op (appended by each new leader). */
public record LogEntry(long term, byte[] command) {
    public static final byte[] NOOP = new byte[0];

    public boolean isNoop() {
        return command.length == 0;
    }

    @Override
    public String toString() {
        return "LogEntry[term=" + term + ", bytes=" + command.length + "]";
    }
}
