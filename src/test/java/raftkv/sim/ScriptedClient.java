package raftkv.sim;

import raftkv.kv.Op;
import raftkv.transport.Message;
import raftkv.transport.Message.ClientReply;
import raftkv.transport.Message.ClientRequest;

/** Sends one request to a chosen node and runs the simulator until it answers or time runs out. */
final class ScriptedClient {
    private final int endpoint;
    private final long clientId;
    private final Simulator sim;
    private final SimNetwork net;
    private final InvariantChecker invariants;
    private long seq;
    private ClientReply last;

    ScriptedClient(int endpoint, long clientId, SimCluster cluster, InvariantChecker invariants) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.sim = cluster.sim();
        this.net = cluster.network();
        this.invariants = invariants;
        net.register(endpoint, m -> {
            if (m instanceof ClientReply r && r.seq() == seq) {
                last = r;
            }
        });
    }

    /** Returns the reply, or null if none arrived within waitMs. */
    ClientReply call(int node, Op op, String key, String value, long waitMs) {
        seq++;
        last = null;
        net.send(new ClientRequest(endpoint, node, clientId, seq, op, key, value));
        long until = sim.now() + waitMs;
        sim.runUntil(until, invariants::afterStep, () -> last != null);
        return last;
    }

    static boolean ok(ClientReply r) {
        return r != null && r.status() == Message.Status.OK;
    }
}
