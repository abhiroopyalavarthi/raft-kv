package raftkv.raft;

/** Outcome of a proposal or a linearizable read. */
public interface RaftCallback {
    /** The command was committed and applied (result is the state machine's output), or the read is safe to serve. */
    void onSuccess(byte[] result);

    /** This node is not (or no longer) leader. leaderHint is 0 when unknown. */
    void onNotLeader(int leaderHint);
}
