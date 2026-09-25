package raftkv.sim;

import raftkv.raft.Clock;

/** Time that only moves when the simulator says so. */
public final class FakeClock implements Clock {
    private long now;

    @Override
    public long now() {
        return now;
    }

    void advanceTo(long t) {
        if (t < now) {
            throw new IllegalStateException("time went backwards: " + t + " < " + now);
        }
        now = t;
    }
}
