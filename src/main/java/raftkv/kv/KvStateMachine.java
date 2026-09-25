package raftkv.kv;

import raftkv.raft.StateMachine;

import java.util.HashMap;
import java.util.Map;

/**
 * The replicated key-value map. Sees only committed commands, in log order.
 *
 * <p>Each client has a session holding the last sequence number it applied and that command's
 * result. A retried command (same clientId and seq) returns the saved result instead of running
 * twice, which is what makes client retries safe.
 */
public final class KvStateMachine implements StateMachine {
    private record Session(long lastSeq, String lastResult) {}

    private final Map<String, String> data = new HashMap<>();
    private final Map<Long, Session> sessions = new HashMap<>();
    private final boolean dedup;
    private long appliedIndex;
    private long executedWrites;

    public KvStateMachine(boolean dedup) {
        this.dedup = dedup;
    }

    @Override
    public byte[] apply(long index, byte[] bytes) {
        appliedIndex = index;
        Command c = Command.decode(bytes);
        Session s = sessions.get(c.clientId());
        if (dedup && s != null && c.seq() <= s.lastSeq()) {
            // Duplicate of a command we already ran: return its original result, change nothing.
            return Command.encodeResult(c.seq() == s.lastSeq() ? s.lastResult() : null);
        }
        String result = switch (c.op()) {
            case PUT -> {
                data.put(c.key(), c.value());
                yield null;
            }
            case DELETE -> {
                data.remove(c.key());
                yield null;
            }
            case GET -> data.get(c.key());
            case STATUS -> throw new IllegalArgumentException("STATUS is not a log command");
        };
        executedWrites++;
        sessions.put(c.clientId(), new Session(Math.max(c.seq(), s == null ? 0 : s.lastSeq()), result));
        return Command.encodeResult(result);
    }

    public String get(String key) {
        return data.get(key);
    }

    public Map<String, String> snapshotData() {
        return Map.copyOf(data);
    }

    public int size() {
        return data.size();
    }

    public long appliedIndex() {
        return appliedIndex;
    }

    /** Commands that actually changed state (duplicates excluded). */
    public long executedWrites() {
        return executedWrites;
    }
}
