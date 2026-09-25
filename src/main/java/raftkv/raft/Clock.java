package raftkv.raft;

/** Milliseconds since some fixed origin. Raft never reads wall-clock time directly. */
public interface Clock {
    long now();
}
