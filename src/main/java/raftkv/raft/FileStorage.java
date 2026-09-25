package raftkv.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

/**
 * On-disk storage for a real node.
 *
 * <pre>
 *   meta  term (8) | votedFor (4) | crc32 (4)            replaced atomically: write tmp, fsync, rename
 *   log   [ length (4) | crc32 (4) | term (8) | command ]*   append-only, fsync after every append
 * </pre>
 *
 * A crash can leave a half-written record at the end of the log; load() detects it by length or
 * checksum and cuts it off. That record was never acknowledged, because we only reply after fsync.
 */
public final class FileStorage implements Storage {
    private static final int HEADER = 8;

    private final Path dir;
    private final Path metaPath;
    private final FileChannel logChannel;
    private long[] offsets = new long[1024];
    private int count;
    private long end;
    private long fsyncs;

    public FileStorage(Path dir) {
        try {
            this.dir = dir;
            Files.createDirectories(dir);
            this.metaPath = dir.resolve("meta");
            this.logChannel = FileChannel.open(dir.resolve("log"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public State load() {
        try {
            long term = 0;
            int votedFor = 0;
            if (Files.exists(metaPath)) {
                ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(metaPath));
                term = b.getLong();
                votedFor = b.getInt();
                int crc = b.getInt();
                if (crc != crc(Arrays.copyOf(b.array(), 12))) {
                    throw new IllegalStateException("corrupt meta file in " + dir);
                }
            }
            List<LogEntry> entries = new ArrayList<>();
            count = 0;
            long size = logChannel.size();
            ByteBuffer all = ByteBuffer.allocate((int) size);
            while (all.hasRemaining() && logChannel.read(all, all.position()) >= 0) {
                // keep reading
            }
            all.flip();
            long pos = 0;
            while (all.remaining() >= HEADER) {
                int len = all.getInt();
                int crc = all.getInt();
                if (len < 8 || len > all.remaining()) {
                    break;
                }
                byte[] payload = new byte[len];
                all.get(payload);
                if (crc(payload) != crc) {
                    break;
                }
                ByteBuffer p = ByteBuffer.wrap(payload);
                long t = p.getLong();
                byte[] command = new byte[len - 8];
                p.get(command);
                entries.add(new LogEntry(t, command));
                recordOffset(pos);
                pos += HEADER + len;
            }
            if (pos != size) {
                // torn write at the tail from a crash mid-append
                logChannel.truncate(pos);
                logChannel.force(true);
            }
            end = pos;
            return new State(term, votedFor, entries);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        try {
            ByteBuffer b = ByteBuffer.allocate(16);
            b.putLong(term).putInt(votedFor);
            b.putInt(crc(Arrays.copyOf(b.array(), 12)));
            b.flip();
            Path tmp = dir.resolve("meta.tmp");
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                while (b.hasRemaining()) {
                    ch.write(b);
                }
                ch.force(true);
            }
            try {
                Files.move(tmp, metaPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, metaPath, StandardCopyOption.REPLACE_EXISTING);
            }
            syncDirectory();
            fsyncs++;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void append(long firstIndex, List<LogEntry> entries) {
        if (firstIndex != count + 1) {
            throw new IllegalStateException("append at " + firstIndex + " but log has " + count);
        }
        int total = 0;
        for (LogEntry e : entries) {
            total += HEADER + 8 + e.command().length;
        }
        ByteBuffer buf = ByteBuffer.allocate(total);
        long pos = end;
        for (LogEntry e : entries) {
            byte[] payload = ByteBuffer.allocate(8 + e.command().length).putLong(e.term()).put(e.command()).array();
            buf.putInt(payload.length).putInt(crc(payload)).put(payload);
            recordOffset(pos);
            pos += HEADER + payload.length;
        }
        buf.flip();
        try {
            long at = end;
            while (buf.hasRemaining()) {
                at += logChannel.write(buf, at);
            }
            logChannel.force(false);
            fsyncs++;
            end = pos;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void truncateFrom(long index) {
        if (index > count) {
            return;
        }
        try {
            long off = offsets[(int) (index - 1)];
            logChannel.truncate(off);
            logChannel.force(true);
            fsyncs++;
            end = off;
            count = (int) (index - 1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long fsyncs() {
        return fsyncs;
    }

    @Override
    public void close() {
        try {
            logChannel.close();
        } catch (IOException ignored) {
            // closing on shutdown
        }
    }

    private void recordOffset(long pos) {
        if (count == offsets.length) {
            offsets = Arrays.copyOf(offsets, offsets.length * 2);
        }
        offsets[count++] = pos;
    }

    private void syncDirectory() {
        // Makes the rename durable on Linux. Some platforms refuse to open a directory; that's fine.
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // best effort
        }
    }

    private static int crc(byte[] data) {
        CRC32 c = new CRC32();
        c.update(data);
        return (int) c.getValue();
    }
}
