package raftkv.raft;

import raftkv.transport.Message;

import java.util.Random;

/**
 * Everything a Raft node needs from the outside world. The simulator and the real
 * TCP runtime each provide one. All calls happen on the node's single thread.
 */
public interface RaftEnv extends Clock {
    int LOG_OFF = 0, LOG_INFO = 1, LOG_DEBUG = 2;

    Random random();

    void send(Message message);

    /** Run task on the node's thread after delayMs (0 = after events already queued). */
    void schedule(long delayMs, Runnable task);

    int logLevel();

    void log(String line);
}
