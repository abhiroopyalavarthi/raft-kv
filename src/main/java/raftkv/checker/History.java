package raftkv.checker;

import raftkv.kv.Op;

import java.util.ArrayList;
import java.util.List;

/** Thread-safe log of every client operation in a run. */
public final class History {
    private final List<Operation> ops = new ArrayList<>();

    public synchronized Operation invoke(long clientId, Op op, String key, String input, long time) {
        Operation o = new Operation(ops.size(), clientId, op, key, input, time);
        ops.add(o);
        return o;
    }

    public synchronized void complete(Operation op, String output, long time) {
        op.complete(output, time);
    }

    public synchronized List<Operation> operations() {
        return List.copyOf(ops);
    }

    public synchronized int completedCount() {
        int n = 0;
        for (Operation o : ops) {
            if (!o.isPending()) {
                n++;
            }
        }
        return n;
    }
}
