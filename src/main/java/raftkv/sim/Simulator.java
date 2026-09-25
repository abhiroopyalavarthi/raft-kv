package raftkv.sim;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.BooleanSupplier;

/**
 * Discrete-event simulator. One priority queue of events ordered by (time, sequence number),
 * one fake clock and one seeded Random. Nothing else in a simulated run is allowed to be a
 * source of time or randomness, so the same seed always replays the same run.
 */
public final class Simulator {
    private record Event(long time, long seq, Runnable action) {}

    private final PriorityQueue<Event> queue =
            new PriorityQueue<>(Comparator.comparingLong(Event::time).thenComparingLong(Event::seq));
    private final FakeClock clock = new FakeClock();
    private final Random random;
    private long nextSeq;
    private long steps;

    public Simulator(long seed) {
        this.random = new Random(seed);
    }

    public long now() {
        return clock.now();
    }

    public FakeClock clock() {
        return clock;
    }

    public Random random() {
        return random;
    }

    public long steps() {
        return steps;
    }

    public void schedule(long delayMs, Runnable action) {
        queue.add(new Event(clock.now() + Math.max(0, delayMs), nextSeq++, action));
    }

    /** Runs the next event. Returns false if there is nothing left to run. */
    public boolean step() {
        Event e = queue.poll();
        if (e == null) {
            return false;
        }
        clock.advanceTo(e.time());
        steps++;
        e.action().run();
        return true;
    }

    /**
     * Runs events up to and including time t. afterEachStep runs after every event; stop is
     * checked after every event and ends the run early when it returns true.
     */
    public void runUntil(long t, Runnable afterEachStep, BooleanSupplier stop) {
        while (!queue.isEmpty() && queue.peek().time() <= t) {
            step();
            if (afterEachStep != null) {
                afterEachStep.run();
            }
            if (stop != null && stop.getAsBoolean()) {
                return;
            }
        }
        if (clock.now() < t) {
            clock.advanceTo(t);
        }
    }

    public void runUntil(long t) {
        runUntil(t, null, null);
    }
}
