package raftkv.raft;

/** Receives committed commands in log order. Must be deterministic. */
public interface StateMachine {
    byte[] apply(long index, byte[] command);
}
