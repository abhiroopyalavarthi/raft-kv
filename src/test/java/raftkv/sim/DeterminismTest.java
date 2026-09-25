package raftkv.sim;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Milestone 0: the same seed always produces exactly the same run. */
class DeterminismTest {

    private static List<String> trace(long seed) {
        List<String> trace = new ArrayList<>();
        RandomizedRun.Result r = RandomizedRun.run(seed, RandomizedRun.Options.defaults().withTracer(trace::add));
        trace.add("result " + r.ok() + " ops=" + r.completed() + " steps=" + r.steps() + " faults=" + r.faultLog());
        return trace;
    }

    @Test
    void sameSeedSameRun() {
        for (long seed : new long[]{1, 7, 12345}) {
            List<String> a = trace(seed);
            List<String> b = trace(seed);
            assertTrue(a.size() > 1000, "a run delivers plenty of messages");
            assertEquals(a, b, "seed " + seed + " must replay identically");
        }
    }

    @Test
    void differentSeedsDifferentRuns() {
        assertNotEquals(trace(1), trace(2));
    }

    @Test
    void simulatorOrdersEventsByTimeThenInsertion() {
        Simulator sim = new Simulator(1);
        List<String> order = new ArrayList<>();
        sim.schedule(10, () -> order.add("b"));
        sim.schedule(5, () -> order.add("a"));
        sim.schedule(10, () -> order.add("c"));
        sim.schedule(10, () -> sim.schedule(0, () -> order.add("d")));
        sim.runUntil(100);
        assertEquals(List.of("a", "b", "c", "d"), order);
        assertEquals(100, sim.now());
    }
}
