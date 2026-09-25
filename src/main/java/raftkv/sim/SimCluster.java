package raftkv.sim;

import raftkv.kv.KvServer;
import raftkv.raft.MemoryStorage;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftEnv;
import raftkv.raft.RaftNode;
import raftkv.raft.RaftObserver;
import raftkv.transport.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

/** N simulated nodes sharing one simulator and network. Nodes can crash and restart from their "disk". */
public final class SimCluster {

    public final class Host {
        public final int id;
        final MemoryStorage storage = new MemoryStorage();
        int incarnation;
        boolean up;
        KvServer server;

        Host(int id) {
            this.id = id;
        }

        public boolean isUp() {
            return up;
        }

        public KvServer server() {
            return server;
        }

        public RaftNode raft() {
            return server.raft();
        }
    }

    private final class HostEnv implements RaftEnv {
        private final Host host;
        private final int incarnation;

        HostEnv(Host host) {
            this.host = host;
            this.incarnation = host.incarnation;
        }

        private boolean alive() {
            return host.up && host.incarnation == incarnation;
        }

        @Override
        public long now() {
            return sim.now();
        }

        @Override
        public Random random() {
            return sim.random();
        }

        @Override
        public void send(Message message) {
            if (alive()) {
                network.send(message);
            }
        }

        @Override
        public void schedule(long delayMs, Runnable task) {
            sim.schedule(delayMs, () -> {
                if (alive()) {
                    task.run();
                }
            });
        }

        @Override
        public int logLevel() {
            return logLevel;
        }

        @Override
        public void log(String line) {
            logSink.accept("t=" + sim.now() + " " + line);
        }
    }

    private final Simulator sim;
    private final SimNetwork network;
    private final RaftConfig config;
    private final int[] members;
    private final Host[] hosts;
    private RaftObserver observer = RaftObserver.NONE;
    private int logLevel = RaftEnv.LOG_OFF;
    private Consumer<String> logSink = s -> {};

    public SimCluster(Simulator sim, SimNetwork network, int size, RaftConfig config) {
        this.sim = sim;
        this.network = network;
        this.config = config;
        this.members = new int[size];
        this.hosts = new Host[size + 1];
        for (int i = 1; i <= size; i++) {
            members[i - 1] = i;
            hosts[i] = new Host(i);
            Host h = hosts[i];
            network.register(i, m -> {
                if (h.up) {
                    h.server.onMessage(m);
                }
            });
        }
    }

    public void setObserver(RaftObserver observer) {
        this.observer = observer;
    }

    public void setLogging(int level, Consumer<String> sink) {
        this.logLevel = level;
        this.logSink = sink;
    }

    public void startAll() {
        for (int i = 1; i < hosts.length; i++) {
            restart(i);
        }
    }

    public void crash(int id) {
        Host h = hosts[id];
        if (!h.up) {
            return;
        }
        h.server.stop();
        h.up = false;
        h.incarnation++;
        network.setDown(id, true);
        if (logLevel > 0) {
            logSink.accept("t=" + sim.now() + " *** CRASH n" + id);
        }
    }

    /** Starts (or restarts) a node from whatever its storage holds. Volatile state starts empty. */
    public void restart(int id) {
        Host h = hosts[id];
        if (h.up) {
            return;
        }
        h.incarnation++;
        h.up = true;
        network.setDown(id, false);
        h.server = new KvServer(id, members, config, new HostEnv(h), h.storage);
        h.server.raft().setObserver(observer);
        if (logLevel > 0) {
            logSink.accept("t=" + sim.now() + " *** START n" + id);
        }
        h.server.start();
    }

    public int size() {
        return members.length;
    }

    public Host host(int id) {
        return hosts[id];
    }

    public List<Host> hosts() {
        List<Host> list = new ArrayList<>();
        for (int i = 1; i < hosts.length; i++) {
            list.add(hosts[i]);
        }
        return list;
    }

    public List<Host> upHosts() {
        List<Host> list = new ArrayList<>();
        for (int i = 1; i < hosts.length; i++) {
            if (hosts[i].up) {
                list.add(hosts[i]);
            }
        }
        return list;
    }

    public int downCount() {
        int n = 0;
        for (int i = 1; i < hosts.length; i++) {
            if (!hosts[i].up) {
                n++;
            }
        }
        return n;
    }

    /** The leader with the highest term among running nodes, or null. */
    public Host leader() {
        Host best = null;
        for (Host h : upHosts()) {
            if (h.raft().isLeader() && (best == null || h.raft().currentTerm() > best.raft().currentTerm())) {
                best = h;
            }
        }
        return best;
    }

    public Simulator sim() {
        return sim;
    }

    public SimNetwork network() {
        return network;
    }

    public int[] members() {
        return members.clone();
    }
}
