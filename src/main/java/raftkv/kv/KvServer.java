package raftkv.kv;

import raftkv.raft.PlantedBug;
import raftkv.raft.RaftCallback;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftEnv;
import raftkv.raft.RaftNode;
import raftkv.raft.Storage;
import raftkv.transport.Message;
import raftkv.transport.Message.ClientReply;
import raftkv.transport.Message.ClientRequest;
import raftkv.transport.Message.Status;

/** Glue between clients and Raft: writes go through the log, reads through ReadIndex. */
public final class KvServer {
    private final RaftNode raft;
    private final KvStateMachine stateMachine;
    private final RaftEnv env;
    private final RaftConfig cfg;

    public KvServer(int id, int[] members, RaftConfig cfg, RaftEnv env, Storage storage) {
        this.env = env;
        this.cfg = cfg;
        this.stateMachine = new KvStateMachine(!cfg.has(PlantedBug.NO_DEDUP));
        this.raft = new RaftNode(id, members, cfg, env, storage, stateMachine);
    }

    public void start() {
        raft.start();
    }

    public void stop() {
        raft.stop();
    }

    public RaftNode raft() {
        return raft;
    }

    public KvStateMachine stateMachine() {
        return stateMachine;
    }

    public void onMessage(Message m) {
        if (m instanceof ClientRequest req) {
            handleClient(req);
        } else {
            raft.onMessage(m);
        }
    }

    private void handleClient(ClientRequest req) {
        if (!raft.isRunning()) {
            return;
        }
        switch (req.op()) {
            case STATUS -> reply(req, Status.OK, raft.status(), 0);
            case GET -> {
                if (cfg.has(PlantedBug.FOLLOWER_READS)) {
                    reply(req, Status.OK, stateMachine.get(req.key()), 0);
                    return;
                }
                raft.linearizableRead(new RaftCallback() {
                    @Override
                    public void onSuccess(byte[] ignored) {
                        reply(req, Status.OK, stateMachine.get(req.key()), 0);
                    }

                    @Override
                    public void onNotLeader(int hint) {
                        reply(req, Status.NOT_LEADER, null, hint);
                    }
                });
            }
            case PUT, DELETE -> {
                Command c = new Command(req.clientId(), req.seq(), req.op(), req.key(), req.value());
                raft.propose(c.encode(), new RaftCallback() {
                    @Override
                    public void onSuccess(byte[] result) {
                        reply(req, Status.OK, Command.decodeResult(result), 0);
                    }

                    @Override
                    public void onNotLeader(int hint) {
                        reply(req, Status.NOT_LEADER, null, hint);
                    }
                });
            }
        }
    }

    private void reply(ClientRequest req, Status status, String value, int hint) {
        env.send(new ClientReply(raft.id(), req.from(), req.clientId(), req.seq(), status, value, hint));
    }
}
