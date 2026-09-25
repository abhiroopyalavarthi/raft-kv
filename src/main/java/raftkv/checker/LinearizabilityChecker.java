package raftkv.checker;

import raftkv.kv.Op;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Checks that a history of put/get/delete operations is linearizable: that there is one total order
 * of the operations, consistent with real time, in which every get returns the latest put.
 *
 * <p>Linearizability is compositional, so each key is checked on its own. Each key is checked with
 * the Wing &amp; Gong search, using the memoization from Lowe ("Testing for linearizability", 2017),
 * which the Knossos and Porcupine checkers also use. Operations that never returned may or may not
 * have taken effect: pending writes can be placed anywhere after they were invoked, and pending
 * reads are dropped because nobody saw their result.
 */
public final class LinearizabilityChecker {

    public record Result(boolean linearizable, boolean timedOut, String key, String explanation) {
        static final Result OK = new Result(true, false, null, null);
    }

    private final long maxStepsPerKey;

    public LinearizabilityChecker() {
        this(5_000_000);
    }

    public LinearizabilityChecker(long maxStepsPerKey) {
        this.maxStepsPerKey = maxStepsPerKey;
    }

    public Result check(List<Operation> history) {
        Map<String, List<Operation>> byKey = new TreeMap<>();
        for (Operation o : history) {
            if (o.op == Op.STATUS || (o.isPending() && o.op == Op.GET)) {
                continue;
            }
            byKey.computeIfAbsent(o.key, k -> new ArrayList<>()).add(o);
        }
        Result timeout = null;
        for (Map.Entry<String, List<Operation>> e : byKey.entrySet()) {
            Result r = checkKey(e.getKey(), e.getValue());
            if (!r.linearizable() && !r.timedOut()) {
                return r;
            }
            if (r.timedOut() && timeout == null) {
                timeout = r;
            }
        }
        return timeout != null ? timeout : Result.OK;
    }

    private static final class Entry {
        final boolean call;
        final int op;
        final long time;
        Entry match;
        Entry prev;
        Entry next;

        Entry(boolean call, int op, long time) {
            this.call = call;
            this.op = op;
            this.time = time;
        }
    }

    private record Frame(Entry entry, String state) {}

    private record CacheKey(BitSet linearized, String state) {}

    private Result checkKey(String key, List<Operation> ops) {
        List<Entry> events = new ArrayList<>(ops.size() * 2);
        for (int i = 0; i < ops.size(); i++) {
            Operation o = ops.get(i);
            Entry call = new Entry(true, i, o.invokeTime);
            Entry ret = new Entry(false, i, o.completeTime());
            call.match = ret;
            ret.match = call;
            events.add(call);
            events.add(ret);
        }
        // Calls sort before returns at the same instant: operations touching the same millisecond count as concurrent.
        events.sort(Comparator.comparingLong((Entry e) -> e.time).thenComparing(e -> e.call ? 0 : 1));
        Entry head = new Entry(false, -1, Long.MIN_VALUE);
        Entry tail = head;
        for (Entry e : events) {
            tail.next = e;
            e.prev = tail;
            tail = e;
        }

        BitSet linearized = new BitSet(ops.size());
        Set<CacheKey> cache = new HashSet<>();
        Deque<Frame> stack = new ArrayDeque<>();
        String state = null;
        Entry entry = head.next;
        long steps = 0;
        int best = 0;
        Entry stuckAt = entry;

        while (head.next != null) {
            if (++steps > maxStepsPerKey) {
                return new Result(false, true, key, "search budget exhausted after " + steps + " steps (" + ops.size() + " ops)");
            }
            if (entry.call) {
                Operation o = ops.get(entry.op);
                String next = null;
                boolean ok;
                switch (o.op) {
                    case PUT -> {
                        ok = true;
                        next = o.input;
                    }
                    case DELETE -> ok = true;
                    case GET -> {
                        ok = Objects.equals(state, o.output());
                        next = state;
                    }
                    default -> throw new IllegalStateException();
                }
                if (ok) {
                    BitSet candidate = (BitSet) linearized.clone();
                    candidate.set(entry.op);
                    if (cache.add(new CacheKey(candidate, next))) {
                        stack.push(new Frame(entry, state));
                        state = next;
                        linearized.set(entry.op);
                        lift(entry);
                        if (stack.size() > best) {
                            best = stack.size();
                        }
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                // Reached the return of an operation we could not place before it: backtrack.
                if (stack.isEmpty()) {
                    return new Result(false, false, key, explain(key, ops, best, stuckAt));
                }
                if (stack.size() == best) {
                    stuckAt = entry;
                }
                Frame f = stack.pop();
                state = f.state();
                linearized.clear(f.entry().op);
                unlift(f.entry());
                entry = f.entry().next;
            }
        }
        return Result.OK;
    }

    private static void lift(Entry call) {
        call.prev.next = call.next;
        if (call.next != null) {
            call.next.prev = call.prev;
        }
        Entry ret = call.match;
        ret.prev.next = ret.next;
        if (ret.next != null) {
            ret.next.prev = ret.prev;
        }
    }

    private static void unlift(Entry call) {
        Entry ret = call.match;
        ret.prev.next = ret;
        if (ret.next != null) {
            ret.next.prev = ret;
        }
        call.prev.next = call;
        if (call.next != null) {
            call.next.prev = call;
        }
    }

    private static String explain(String key, List<Operation> ops, int best, Entry stuckAt) {
        StringBuilder sb = new StringBuilder();
        sb.append("key '").append(key).append("' has no valid order (").append(ops.size())
                .append(" ops, longest valid prefix ").append(best).append(")");
        if (stuckAt != null && stuckAt.op >= 0) {
            Operation o = ops.get(stuckAt.op);
            sb.append("\n  cannot place: ").append(o);
            List<Operation> around = new ArrayList<>();
            for (Operation x : ops) {
                if (x.invokeTime <= o.completeTime() && x.completeTime() >= o.invokeTime - 200) {
                    around.add(x);
                }
            }
            around.sort(Comparator.comparingLong(x -> x.invokeTime));
            int shown = 0;
            for (Operation x : around) {
                if (shown++ >= 15) {
                    break;
                }
                sb.append("\n    ").append(x);
            }
        }
        return sb.toString();
    }
}
