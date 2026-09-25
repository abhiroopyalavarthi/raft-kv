package raftkv.raft;

/**
 * Bugs that can be switched on deliberately to prove the tests catch them.
 * Never enabled in normal runs.
 */
public enum PlantedBug {
    /** Commit entries from earlier terms by counting replicas (the Figure 8 mistake). */
    COMMIT_PREVIOUS_TERM_ENTRIES,
    /** Leader answers reads from local state without confirming it is still leader. */
    READ_WITHOUT_QUORUM_CHECK,
    /** Any node, including followers, answers reads from its own state machine. */
    FOLLOWER_READS,
    /** The key-value store applies retried writes again instead of deduplicating them. */
    NO_DEDUP
}
