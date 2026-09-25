package raftkv.sim;

import org.junit.jupiter.api.Test;
import raftkv.raft.RaftConfig;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Milestone 5: thousands of seeded runs with random crashes, restarts, partitions, loss, delay,
 * reordering and duplication. Safety rules are checked after every step; every history is checked
 * for linearizability. Run more with ./gradlew test -Druns=100000.
 */
class RandomizedSafetyTest {

    static void runMany(String name, long runs, RandomizedRun.Options opts) {
        long t0 = System.nanoTime();
        AtomicLong ops = new AtomicLong();
        AtomicLong elections = new AtomicLong();
        AtomicLong steps = new AtomicLong();
        List<RandomizedRun.Result> failures = LongStream.rangeClosed(1, runs).parallel()
                .mapToObj(seed -> {
                    RandomizedRun.Result r = RandomizedRun.run(seed, opts);
                    ops.addAndGet(r.completed());
                    elections.addAndGet(r.leaderElections());
                    steps.addAndGet(r.steps());
                    return r;
                })
                .filter(r -> !r.ok()).toList();
        System.out.printf("%s: %,d runs in %.1f s, %,d client operations checked, %,d leader elections, %,d events, %d violations%n",
                name, runs, (System.nanoTime() - t0) / 1e9, ops.get(), elections.get(), steps.get(), failures.size());
        assertTrue(failures.isEmpty(), () -> {
            StringBuilder sb = new StringBuilder();
            for (RandomizedRun.Result r : failures.subList(0, Math.min(3, failures.size()))) {
                sb.append("\nseed ").append(r.seed()).append(": ").append(r.violations())
                        .append("\n  replay: ./gradlew sim --args=\"--seed ").append(r.seed()).append(" --verbose\"");
            }
            return failures.size() + " failing seeds" + sb;
        });
    }

    @Test
    void randomizedFaultRunsHaveZeroViolations() {
        long runs = Long.getLong("runs", 10_000);
        runMany("randomized (batching)", runs, RandomizedRun.Options.defaults());
    }

    @Test
    void randomizedFaultRunsWithoutBatching() {
        long runs = Math.max(100, Long.getLong("runs", 10_000) / 10);
        runMany("randomized (no batching)", runs, RandomizedRun.Options.defaults().withRaft(RaftConfig.defaults().withBatching(false)));
    }
}
