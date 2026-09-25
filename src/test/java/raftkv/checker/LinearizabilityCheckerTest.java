package raftkv.checker;

import org.junit.jupiter.api.Test;
import raftkv.kv.Op;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LinearizabilityCheckerTest {
    private final List<Operation> ops = new ArrayList<>();

    private void put(String v, long start, long end) {
        op(Op.PUT, v, null, start, end);
    }

    private void get(String saw, long start, long end) {
        op(Op.GET, null, saw, start, end);
    }

    private void op(Op kind, String in, String out, long start, long end) {
        Operation o = new Operation(ops.size(), ops.size(), kind, "x", in, start);
        if (end != Operation.PENDING) {
            o.complete(out, end);
        }
        ops.add(o);
    }

    private boolean linearizable() {
        return new LinearizabilityChecker().check(ops).linearizable();
    }

    @Test
    void sequentialHistoryIsLinearizable() {
        put("a", 0, 10);
        get("a", 20, 30);
        put("b", 40, 50);
        get("b", 60, 70);
        assertTrue(linearizable());
    }

    @Test
    void staleReadAfterCompletedWriteIsNot() {
        put("a", 0, 10);
        put("b", 20, 30);
        get("a", 40, 50);
        assertFalse(linearizable());
    }

    @Test
    void readConcurrentWithWriteMaySeeEitherValue() {
        put("a", 0, 10);
        put("b", 20, 100);
        get("a", 30, 40);
        get("b", 50, 60);
        assertTrue(linearizable());
    }

    @Test
    void readsMustNotGoBackInTime() {
        put("a", 0, 10);
        put("b", 20, 100);
        get("b", 30, 40);
        get("a", 50, 60); // saw b, then a: b's write can't be undone
        assertFalse(linearizable());
    }

    @Test
    void readOfMissingKeyBeforeFirstWrite() {
        get(null, 0, 5);
        put("a", 10, 20);
        get(null, 30, 40);
        assertFalse(linearizable());
    }

    @Test
    void pendingWriteMayHaveTakenEffect() {
        put("a", 0, 10);
        put("b", 20, Operation.PENDING); // client never heard back
        get("b", 50, 60);
        get("b", 70, 80);
        assertTrue(linearizable());
    }

    @Test
    void pendingWriteMayAlsoNeverHappen() {
        put("a", 0, 10);
        put("b", 20, Operation.PENDING);
        get("a", 50, 60);
        assertTrue(linearizable());
    }

    @Test
    void deleteThenReadNothing() {
        put("a", 0, 10);
        op(Op.DELETE, null, null, 20, 30);
        get(null, 40, 50);
        assertTrue(linearizable());
        get("a", 60, 70);
        assertFalse(linearizable());
    }

    @Test
    void valueThatWasNeverWrittenIsCaught() {
        put("a", 0, 10);
        get("zzz", 20, 30);
        assertFalse(linearizable());
    }

    @Test
    void largeConcurrentHistoryIsCheckedQuickly() {
        // 8 clients, 400 overlapping ops, all consistent with one register.
        long t = 0;
        String current = null;
        for (int i = 0; i < 400; i++) {
            if (i % 3 == 0) {
                current = "v" + i;
                put(current, t, t + 25);
            } else {
                get(current, t + 5, t + 30);
            }
            t += 10;
        }
        long start = System.nanoTime();
        assertTrue(linearizable());
        assertTrue(System.nanoTime() - start < 5_000_000_000L);
    }
}
