package raftkv.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/** A write as stored in the Raft log. (clientId, seq) identifies it for deduplication. */
public record Command(long clientId, long seq, Op op, String key, String value) {

    public byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(32 + key.length() + (value == null ? 0 : value.length()));
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeLong(clientId);
            out.writeLong(seq);
            out.writeByte(op.ordinal());
            out.writeUTF(key);
            writeNullable(out, value);
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Command decode(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            return new Command(in.readLong(), in.readLong(), Op.values()[in.readByte()], in.readUTF(), readNullable(in));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static byte[] encodeResult(String value) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            writeNullable(new DataOutputStream(bytes), value);
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String decodeResult(byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return readNullable(new DataInputStream(new ByteArrayInputStream(data)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void writeNullable(DataOutputStream out, String s) throws IOException {
        out.writeBoolean(s != null);
        if (s != null) {
            out.writeUTF(s);
        }
    }

    static String readNullable(DataInputStream in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }
}
