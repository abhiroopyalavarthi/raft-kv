package raftkv.transport;

import org.junit.jupiter.api.Test;
import raftkv.kv.Op;
import raftkv.raft.LogEntry;
import raftkv.transport.Message.AppendEntries;
import raftkv.transport.Message.AppendEntriesReply;
import raftkv.transport.Message.ClientReply;
import raftkv.transport.Message.ClientRequest;
import raftkv.transport.Message.RequestVote;
import raftkv.transport.Message.RequestVoteReply;
import raftkv.transport.Message.Status;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CodecTest {

    private static Message roundTrip(Message m) {
        return Codec.decode(Codec.encode(m));
    }

    @Test
    void everyMessageTypeRoundTrips() {
        List<Message> msgs = List.of(
                new RequestVote(1, 2, 5, 10, 4),
                new RequestVoteReply(2, 1, 5, true),
                new AppendEntriesReply(3, 1, 5, false, 0, 7, 3, 42),
                new ClientRequest(1_000_001, 1, 99, 3, Op.PUT, "key", "value"),
                new ClientRequest(1_000_001, 1, 99, 4, Op.GET, "key", null),
                new ClientReply(1, 1_000_001, 99, 4, Status.NOT_LEADER, null, 3));
        for (Message m : msgs) {
            assertEquals(m, roundTrip(m));
        }
    }

    @Test
    void appendEntriesKeepsEntryBytes() {
        AppendEntries ae = new AppendEntries(1, 2, 5, 9, 4,
                List.of(new LogEntry(5, new byte[]{1, 2, 3}), new LogEntry(5, LogEntry.NOOP)), 8, 17);
        AppendEntries back = (AppendEntries) roundTrip(ae);
        assertEquals(ae.term(), back.term());
        assertEquals(ae.prevLogIndex(), back.prevLogIndex());
        assertEquals(ae.leaderCommit(), back.leaderCommit());
        assertEquals(ae.seq(), back.seq());
        assertEquals(2, back.entries().size());
        assertTrue(Arrays.equals(new byte[]{1, 2, 3}, back.entries().get(0).command()));
        assertTrue(back.entries().get(1).isNoop());
    }
}
