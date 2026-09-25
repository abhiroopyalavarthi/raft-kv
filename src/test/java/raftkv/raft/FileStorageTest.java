package raftkv.raft;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileStorageTest {

    private static LogEntry e(long term, String s) {
        return new LogEntry(term, s.getBytes());
    }

    @Test
    void termVoteAndLogSurviveReopen() throws IOException {
        Path dir = Files.createTempDirectory("raft-fs");
        FileStorage s = new FileStorage(dir);
        s.load();
        s.saveTermAndVote(7, 3);
        s.append(1, List.of(e(1, "a"), e(1, "b")));
        s.append(3, List.of(e(7, "c")));
        s.close();

        FileStorage reopened = new FileStorage(dir);
        Storage.State st = reopened.load();
        assertEquals(7, st.term());
        assertEquals(3, st.votedFor());
        assertEquals(3, st.log().size());
        assertEquals("c", new String(st.log().get(2).command()));
        assertEquals(7, st.log().get(2).term());
        reopened.close();
    }

    @Test
    void truncateThenAppendIsDurable() throws IOException {
        Path dir = Files.createTempDirectory("raft-fs");
        FileStorage s = new FileStorage(dir);
        s.load();
        s.append(1, List.of(e(1, "a"), e(1, "b"), e(1, "c")));
        s.truncateFrom(2);
        s.append(2, List.of(e(2, "x")));
        s.close();

        FileStorage r = new FileStorage(dir);
        List<LogEntry> log = r.load().log();
        assertEquals(2, log.size());
        assertEquals("a", new String(log.get(0).command()));
        assertEquals("x", new String(log.get(1).command()));
        assertEquals(2, log.get(1).term());
        r.close();
    }

    @Test
    void tornWriteAtTheTailIsDiscarded() throws IOException {
        Path dir = Files.createTempDirectory("raft-fs");
        FileStorage s = new FileStorage(dir);
        s.load();
        s.append(1, List.of(e(1, "first"), e(1, "second"), e(1, "third")));
        s.close();
        // Simulate a crash in the middle of writing the last record.
        Path log = dir.resolve("log");
        long size = Files.size(log);
        try (FileChannel ch = FileChannel.open(log, StandardOpenOption.WRITE)) {
            ch.truncate(size - 3);
        }

        FileStorage r = new FileStorage(dir);
        List<LogEntry> entries = r.load().log();
        assertEquals(2, entries.size(), "the half-written record is dropped");
        r.append(3, List.of(e(2, "again")));
        r.close();

        FileStorage r2 = new FileStorage(dir);
        List<LogEntry> after = r2.load().log();
        assertEquals(3, after.size());
        assertEquals("again", new String(after.get(2).command()));
        r2.close();
    }

    @Test
    void corruptedRecordStopsTheLog() throws IOException {
        Path dir = Files.createTempDirectory("raft-fs");
        FileStorage s = new FileStorage(dir);
        s.load();
        s.append(1, List.of(e(1, "aaaa"), e(1, "bbbb")));
        s.close();
        Path log = dir.resolve("log");
        byte[] bytes = Files.readAllBytes(log);
        bytes[bytes.length - 1] ^= 0x55; // flip bits in the last record's payload
        Files.write(log, bytes);

        FileStorage r = new FileStorage(dir);
        assertEquals(1, r.load().log().size(), "checksum catches the damaged record");
        r.close();
    }
}
