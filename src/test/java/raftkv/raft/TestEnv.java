package raftkv.raft;

import raftkv.transport.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** A hand-cranked environment for unit-testing one RaftNode: manual time, captured messages. */
final class TestEnv implements RaftEnv {
    private record Task(long time, long seq, Runnable run) {}

    long now;
    private long seq;
    private final Random random = new Random(1);
    private final List<Task> tasks = new ArrayList<>();
    final List<Message> sent = new ArrayList<>();

    @Override
    public long now() {
        return now;
    }

    @Override
    public Random random() {
        return random;
    }

    @Override
    public void send(Message message) {
        sent.add(message);
    }

    @Override
    public void schedule(long delayMs, Runnable task) {
        tasks.add(new Task(now + delayMs, seq++, task));
    }

    @Override
    public int logLevel() {
        return LOG_OFF;
    }

    @Override
    public void log(String line) {
    }

    /** Runs every task due within the next ms milliseconds, in time order. */
    void advance(long ms) {
        long target = now + ms;
        while (true) {
            Task next = null;
            for (Task t : tasks) {
                if (t.time() <= target && (next == null || t.time() < next.time()
                        || (t.time() == next.time() && t.seq() < next.seq()))) {
                    next = t;
                }
            }
            if (next == null) {
                break;
            }
            tasks.remove(next);
            now = Math.max(now, next.time());
            next.run().run();
        }
        now = target;
    }

    <T extends Message> List<T> sent(Class<T> type) {
        List<T> out = new ArrayList<>();
        for (Message m : sent) {
            if (type.isInstance(m)) {
                out.add(type.cast(m));
            }
        }
        return out;
    }

    <T extends Message> T last(Class<T> type) {
        List<T> all = sent(type);
        if (all.isEmpty()) {
            throw new AssertionError("no " + type.getSimpleName() + " was sent");
        }
        return all.get(all.size() - 1);
    }
}
