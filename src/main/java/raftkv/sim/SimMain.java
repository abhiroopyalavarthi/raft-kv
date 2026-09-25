package raftkv.sim;

import raftkv.raft.PlantedBug;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftEnv;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

/**
 * Command-line driver for randomized runs.
 * <pre>
 *   raft-kv sim --seeds 10000            run seeds 1..10000 in parallel
 *   raft-kv sim --seed 42 --verbose      replay one seed with every node's log
 *   raft-kv sim --seeds 500 --bug FOLLOWER_READS   show that a planted bug gets caught
 * </pre>
 */
public final class SimMain {

    public static void main(String[] args) {
        long seeds = 1000;
        long start = 1;
        Long single = null;
        boolean verbose = false;
        boolean unbatched = false;
        Set<PlantedBug> bugs = EnumSet.noneOf(PlantedBug.class);
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--seeds" -> seeds = Long.parseLong(args[++i]);
                case "--start" -> start = Long.parseLong(args[++i]);
                case "--seed" -> single = Long.parseLong(args[++i]);
                case "--verbose" -> verbose = true;
                case "--unbatched" -> unbatched = true;
                case "--bug" -> bugs.add(PlantedBug.valueOf(args[++i]));
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        RandomizedRun.Options opts = RandomizedRun.Options.defaults()
                .withRaft(RaftConfig.defaults().withBatching(!unbatched).withBugs(bugs));

        if (single != null) {
            RandomizedRun.Options o = verbose ? opts.withLogging(RaftEnv.LOG_DEBUG, System.out::println) : opts;
            RandomizedRun.Result r = RandomizedRun.run(single, o);
            System.out.println();
            System.out.println("faults:\n" + r.faultLog());
            printResult(r);
            System.exit(r.ok() ? 0 : 1);
            return;
        }

        long t0 = System.nanoTime();
        AtomicLong done = new AtomicLong();
        AtomicLong ops = new AtomicLong();
        AtomicLong steps = new AtomicLong();
        AtomicLong elections = new AtomicLong();
        ConcurrentLinkedQueue<RandomizedRun.Result> failures = new ConcurrentLinkedQueue<>();
        long total = seeds;
        LongStream.range(start, start + seeds).parallel().forEach(seed -> {
            RandomizedRun.Result r = RandomizedRun.run(seed, opts);
            ops.addAndGet(r.completed());
            steps.addAndGet(r.steps());
            elections.addAndGet(r.leaderElections());
            if (!r.ok()) {
                failures.add(r);
            }
            long d = done.incrementAndGet();
            if (d % 1000 == 0) {
                System.out.printf("  %d/%d runs, %d failed%n", d, total, failures.size());
            }
        });
        double secs = (System.nanoTime() - t0) / 1e9;
        System.out.printf("%n%d randomized runs in %.1f s: %d client operations, %d leader elections, %d simulated events%n",
                seeds, secs, ops.get(), elections.get(), steps.get());
        System.out.printf("violations: %d%n", failures.size());
        List<RandomizedRun.Result> sorted = failures.stream().sorted((a, b) -> Long.compare(a.seed(), b.seed())).toList();
        for (RandomizedRun.Result r : sorted.subList(0, Math.min(5, sorted.size()))) {
            printResult(r);
        }
        if (!sorted.isEmpty()) {
            System.out.println("replay with: raft-kv sim --seed " + sorted.get(0).seed() + " --verbose"
                    + (unbatched ? " --unbatched" : "") + bugs.stream().map(b -> " --bug " + b).reduce("", String::concat));
        }
        System.exit(failures.isEmpty() ? 0 : 1);
    }

    static void printResult(RandomizedRun.Result r) {
        System.out.printf("seed %d: %s  (%d ops completed, %d elections, max term %d, %d entries committed)%n",
                r.seed(), r.ok() ? "OK" : "FAILED", r.completed(), r.leaderElections(), r.maxTerm(), r.committed());
        for (String v : r.violations()) {
            System.out.println("  " + v);
        }
    }
}
