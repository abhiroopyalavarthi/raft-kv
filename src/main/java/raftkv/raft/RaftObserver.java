package raftkv.raft;

/** Hooks used by the simulator's invariant checker. */
public interface RaftObserver {
    RaftObserver NONE = new RaftObserver() {};

    default void onRoleChange(RaftNode node, Role from, Role to) {}

    default void onCommit(RaftNode node, long oldCommitIndex, long newCommitIndex) {}

    default void onApply(RaftNode node, long index, LogEntry entry) {}
}
