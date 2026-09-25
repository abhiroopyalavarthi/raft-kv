package raftkv.kv;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KvStateMachineTest {

    private static byte[] cmd(long client, long seq, Op op, String key, String value) {
        return new Command(client, seq, op, key, value).encode();
    }

    @Test
    void putGetDelete() {
        KvStateMachine sm = new KvStateMachine(true);
        sm.apply(1, cmd(1, 1, Op.PUT, "a", "1"));
        assertEquals("1", sm.get("a"));
        sm.apply(2, cmd(1, 2, Op.DELETE, "a", null));
        assertNull(sm.get("a"));
    }

    @Test
    void retriedWriteAppliesExactlyOnce() {
        KvStateMachine sm = new KvStateMachine(true);
        sm.apply(1, cmd(7, 1, Op.PUT, "k", "first"));
        sm.apply(2, cmd(8, 1, Op.PUT, "k", "other client"));
        // Client 7's retry of seq 1 lands in the log again (its first reply was lost).
        sm.apply(3, cmd(7, 1, Op.PUT, "k", "first"));
        assertEquals("other client", sm.get("k"), "the duplicate must not overwrite a later write");
        assertEquals(2, sm.executedWrites());
    }

    @Test
    void delayedOlderCommandIsIgnored() {
        KvStateMachine sm = new KvStateMachine(true);
        sm.apply(1, cmd(7, 1, Op.PUT, "k", "v1"));
        sm.apply(2, cmd(7, 2, Op.PUT, "k", "v2"));
        sm.apply(3, cmd(7, 1, Op.PUT, "k", "v1")); // a stale copy of seq 1 arrives late
        assertEquals("v2", sm.get("k"));
    }

    @Test
    void withoutDedupTheDuplicateRunsAgain() {
        KvStateMachine sm = new KvStateMachine(false);
        sm.apply(1, cmd(7, 1, Op.PUT, "k", "first"));
        sm.apply(2, cmd(8, 1, Op.PUT, "k", "other client"));
        sm.apply(3, cmd(7, 1, Op.PUT, "k", "first"));
        assertEquals("first", sm.get("k"));
        assertEquals(3, sm.executedWrites());
    }
}
