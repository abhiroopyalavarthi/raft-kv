package raftkv.sim;

import raftkv.checker.History;
import raftkv.checker.Operation;
import raftkv.kv.Op;
import raftkv.transport.Message;
import raftkv.transport.Message.ClientReply;
import raftkv.transport.Message.ClientRequest;

import java.util.Random;

/**
 * A simulated client. Issues one operation at a time, records it in the history, and retries
 * with the same (clientId, seq) until it gets an answer: on NOT_LEADER it follows the hint, on
 * timeout it tries a random node. It never gives up on an operation, so an unfinished one is
 * truly "outcome unknown" for the linearizability checker.
 */
public final class SimClient {
    private final int endpoint;
    private final long clientId;
    private final Simulator sim;
    private final SimNetwork network;
    private final History history;
    private final Random random;
    private final int clusterSize;
    private final String[] keys;
    private final long requestTimeoutMs;
    private long stopIssuingAt = Long.MAX_VALUE;

    private long seq;
    private Operation current;
    private int target;
    private long attempt;
    private long completed;

    public SimClient(int endpoint, long clientId, Simulator sim, SimNetwork network, History history,
                     int clusterSize, String[] keys, long requestTimeoutMs) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.sim = sim;
        this.network = network;
        this.history = history;
        this.random = sim.random();
        this.clusterSize = clusterSize;
        this.keys = keys;
        this.requestTimeoutMs = requestTimeoutMs;
        this.target = 1 + random.nextInt(clusterSize);
        network.register(endpoint, this::onMessage);
    }

    public void start(long delayMs) {
        sim.schedule(delayMs, this::nextOp);
    }

    public void stopIssuingAt(long t) {
        stopIssuingAt = t;
    }

    public boolean idle() {
        return current == null;
    }

    public Operation current() {
        return current;
    }

    public long completed() {
        return completed;
    }

    private void nextOp() {
        if (sim.now() >= stopIssuingAt) {
            return;
        }
        seq++;
        int r = random.nextInt(100);
        Op op = r < 45 ? Op.PUT : r < 90 ? Op.GET : Op.DELETE;
        String key = keys[random.nextInt(keys.length)];
        String value = op == Op.PUT ? "c" + clientId + "-" + seq : null;
        current = history.invoke(clientId, op, key, value, sim.now());
        send();
    }

    private void send() {
        long a = ++attempt;
        Operation o = current;
        network.send(new ClientRequest(endpoint, target, clientId, seq, o.op, o.key, o.input));
        sim.schedule(requestTimeoutMs, () -> {
            if (a == attempt && current == o) {
                target = 1 + random.nextInt(clusterSize);
                send();
            }
        });
    }

    private void onMessage(Message m) {
        if (!(m instanceof ClientReply r) || current == null || r.seq() != seq || r.clientId() != clientId) {
            return;
        }
        if (r.status() == Message.Status.OK) {
            history.complete(current, current.op == Op.GET ? r.value() : null, sim.now());
            current = null;
            completed++;
            sim.schedule(random.nextInt(10), this::nextOp);
        } else {
            target = r.leaderHint() > 0 && r.leaderHint() != r.from() ? r.leaderHint() : 1 + random.nextInt(clusterSize);
            long a = ++attempt; // cancel the pending timeout
            Operation o = current;
            sim.schedule(5 + random.nextInt(10), () -> {
                if (a == attempt && current == o) {
                    send();
                }
            });
        }
    }
}
