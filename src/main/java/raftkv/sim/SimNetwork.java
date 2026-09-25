package raftkv.sim;

import raftkv.transport.Message;

import java.util.Arrays;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * A network that loses, delays, reorders and duplicates messages, and can be partitioned.
 * Reordering falls out of random per-message delays. Every random choice comes from the
 * simulator's seeded Random.
 */
public final class SimNetwork {
    private final Simulator sim;
    private final Random random;
    private final TreeMap<Integer, Consumer<Message>> endpoints = new TreeMap<>();
    private final int[] partitionGroup = new int[64];
    private final boolean[] down = new boolean[64];
    private double dropRate;
    private double duplicateRate;
    private int minDelayMs = 1;
    private int maxDelayMs = 10;
    private Consumer<String> tracer;
    private long sent;
    private long delivered;

    public SimNetwork(Simulator sim) {
        this.sim = sim;
        this.random = sim.random();
    }

    public void register(int endpoint, Consumer<Message> handler) {
        endpoints.put(endpoint, handler);
    }

    public void setDropRate(double p) {
        dropRate = p;
    }

    public double dropRate() {
        return dropRate;
    }

    public void setDuplicateRate(double p) {
        duplicateRate = p;
    }

    public void setDelay(int minMs, int maxMs) {
        minDelayMs = minMs;
        maxDelayMs = Math.max(minMs, maxMs);
    }

    public void setTracer(Consumer<String> tracer) {
        this.tracer = tracer;
    }

    /** Only nodes are ever partitioned or down; client endpoints (ids >= 64) always reach everyone. */
    public void setDown(int node, boolean isDown) {
        down[node] = isDown;
    }

    /** Nodes with the same group number can talk to each other. */
    public void partition(int[] groupOfNode) {
        Arrays.fill(partitionGroup, 0);
        for (int i = 0; i < groupOfNode.length && i < partitionGroup.length; i++) {
            partitionGroup[i] = groupOfNode[i];
        }
    }

    public void heal() {
        Arrays.fill(partitionGroup, 0);
    }

    public void send(Message m) {
        sent++;
        if (!reachable(m.from(), m.to()) || random.nextDouble() < dropRate) {
            return;
        }
        deliverLater(m);
        if (duplicateRate > 0 && random.nextDouble() < duplicateRate) {
            deliverLater(m);
        }
    }

    private void deliverLater(Message m) {
        int delay = minDelayMs + random.nextInt(maxDelayMs - minDelayMs + 1);
        sim.schedule(delay, () -> deliver(m));
    }

    private void deliver(Message m) {
        if (!reachable(m.from(), m.to())) {
            return;
        }
        Consumer<Message> handler = endpoints.get(m.to());
        if (handler == null) {
            return;
        }
        delivered++;
        if (tracer != null) {
            tracer.accept("t=" + sim.now() + " " + m.from() + "->" + m.to() + " " + m.getClass().getSimpleName());
        }
        handler.accept(m);
    }

    private boolean reachable(int from, int to) {
        boolean fromNode = from < 64;
        boolean toNode = to < 64;
        if ((fromNode && down[from]) || (toNode && down[to])) {
            return false;
        }
        return !(fromNode && toNode) || partitionGroup[from] == partitionGroup[to];
    }

    public long sent() {
        return sent;
    }

    public long delivered() {
        return delivered;
    }
}
