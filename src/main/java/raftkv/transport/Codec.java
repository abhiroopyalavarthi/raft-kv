package raftkv.transport;

import raftkv.kv.Op;
import raftkv.raft.LogEntry;
import raftkv.transport.Message.AppendEntries;
import raftkv.transport.Message.AppendEntriesReply;
import raftkv.transport.Message.ClientReply;
import raftkv.transport.Message.ClientRequest;
import raftkv.transport.Message.RequestVote;
import raftkv.transport.Message.RequestVoteReply;
import raftkv.transport.Message.Status;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** Binary wire format. Each frame on a socket is: int length, then the encoded message. */
public final class Codec {
    private static final byte REQUEST_VOTE = 1, VOTE_REPLY = 2, APPEND = 3, APPEND_REPLY = 4,
            CLIENT_REQUEST = 5, CLIENT_REPLY = 6;

    private Codec() {}

    public static byte[] encode(Message m) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(64);
            DataOutputStream out = new DataOutputStream(bytes);
            switch (m) {
                case RequestVote rv -> {
                    out.writeByte(REQUEST_VOTE);
                    header(out, m);
                    out.writeLong(rv.term());
                    out.writeLong(rv.lastLogIndex());
                    out.writeLong(rv.lastLogTerm());
                }
                case RequestVoteReply r -> {
                    out.writeByte(VOTE_REPLY);
                    header(out, m);
                    out.writeLong(r.term());
                    out.writeBoolean(r.voteGranted());
                }
                case AppendEntries ae -> {
                    out.writeByte(APPEND);
                    header(out, m);
                    out.writeLong(ae.term());
                    out.writeLong(ae.prevLogIndex());
                    out.writeLong(ae.prevLogTerm());
                    out.writeLong(ae.leaderCommit());
                    out.writeLong(ae.seq());
                    out.writeInt(ae.entries().size());
                    for (LogEntry e : ae.entries()) {
                        out.writeLong(e.term());
                        out.writeInt(e.command().length);
                        out.write(e.command());
                    }
                }
                case AppendEntriesReply r -> {
                    out.writeByte(APPEND_REPLY);
                    header(out, m);
                    out.writeLong(r.term());
                    out.writeBoolean(r.success());
                    out.writeLong(r.matchIndex());
                    out.writeLong(r.conflictIndex());
                    out.writeLong(r.conflictTerm());
                    out.writeLong(r.seq());
                }
                case ClientRequest c -> {
                    out.writeByte(CLIENT_REQUEST);
                    header(out, m);
                    out.writeLong(c.clientId());
                    out.writeLong(c.seq());
                    out.writeByte(c.op().ordinal());
                    writeString(out, c.key());
                    writeString(out, c.value());
                }
                case ClientReply c -> {
                    out.writeByte(CLIENT_REPLY);
                    header(out, m);
                    out.writeLong(c.clientId());
                    out.writeLong(c.seq());
                    out.writeByte(c.status().ordinal());
                    writeString(out, c.value());
                    out.writeInt(c.leaderHint());
                }
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Message decode(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            byte type = in.readByte();
            int from = in.readInt();
            int to = in.readInt();
            return switch (type) {
                case REQUEST_VOTE -> new RequestVote(from, to, in.readLong(), in.readLong(), in.readLong());
                case VOTE_REPLY -> new RequestVoteReply(from, to, in.readLong(), in.readBoolean());
                case APPEND -> {
                    long term = in.readLong(), prevIndex = in.readLong(), prevTerm = in.readLong();
                    long commit = in.readLong(), seq = in.readLong();
                    int n = in.readInt();
                    List<LogEntry> entries = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        long t = in.readLong();
                        byte[] cmd = new byte[in.readInt()];
                        in.readFully(cmd);
                        entries.add(new LogEntry(t, cmd));
                    }
                    yield new AppendEntries(from, to, term, prevIndex, prevTerm, entries, commit, seq);
                }
                case APPEND_REPLY -> new AppendEntriesReply(from, to, in.readLong(), in.readBoolean(),
                        in.readLong(), in.readLong(), in.readLong(), in.readLong());
                case CLIENT_REQUEST -> new ClientRequest(from, to, in.readLong(), in.readLong(),
                        Op.values()[in.readByte()], readString(in), readString(in));
                case CLIENT_REPLY -> new ClientReply(from, to, in.readLong(), in.readLong(),
                        Status.values()[in.readByte()], readString(in), in.readInt());
                default -> throw new IOException("unknown message type " + type);
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void header(DataOutputStream out, Message m) throws IOException {
        out.writeInt(m.from());
        out.writeInt(m.to());
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        out.writeBoolean(s != null);
        if (s != null) {
            out.writeUTF(s);
        }
    }

    private static String readString(DataInputStream in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }
}
