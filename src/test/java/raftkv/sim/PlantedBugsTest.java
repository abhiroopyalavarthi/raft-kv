package raftkv.sim;

import org.junit.jupiter.api.Test;
import raftkv.raft.PlantedBug;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Milestone 6: to trust the checkers, plant known bugs and make sure the randomized runs catch them. */
class PlantedBugsTest {

    static List<RandomizedRun.Result> failures(PlantedBug bug, long seeds) {
        RandomizedRun.Options o = RandomizedRun.Options.defaults().withBugs(EnumSet.of(bug));
        return LongStream.rangeClosed(1, seeds).parallel().mapToObj(s -> RandomizedRun.run(s, o))
                .filter(r -> !r.ok()).toList();
    }

    static long linearizabilityFailures(List<RandomizedRun.Result> rs) {
        return rs.stream().filter(r -> r.violations().stream().anyMatch(v -> v.startsWith("Linearizability violated"))).count();
    }

    @Test
    void followerReadsAreCaughtByLinearizabilityChecker() {
        List<RandomizedRun.Result> f = failures(PlantedBug.FOLLOWER_READS, 200);
        System.out.println("FOLLOWER_READS: caught in " + f.size() + "/200 runs");
        assertTrue(linearizabilityFailures(f) > 0);
    }

    @Test
    void leaderReadsWithoutQuorumCheckAreCaught() {
        List<RandomizedRun.Result> f = failures(PlantedBug.READ_WITHOUT_QUORUM_CHECK, 1000);
        System.out.println("READ_WITHOUT_QUORUM_CHECK: caught in " + f.size() + "/1000 runs");
        assertTrue(linearizabilityFailures(f) > 0);
    }

    @Test
    void missingDeduplicationIsCaught() {
        List<RandomizedRun.Result> f = failures(PlantedBug.NO_DEDUP, 300);
        System.out.println("NO_DEDUP: caught in " + f.size() + "/300 runs");
        assertTrue(linearizabilityFailures(f) > 0);
    }
}
