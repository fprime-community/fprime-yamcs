package gov.nasa.jpl.fprime.yamcs.packet;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Codec for the F´ {@code Fw::FilePacket} wire format, prefixed with the
 * F´ {@code FwPacketDescriptorType} descriptor word.
 *
 * <p>Wire format reference: {@code Fw/FilePacket/FilePacket.hpp} in
 * <a href="https://github.com/nasa/fprime">nasa/fprime</a>.
 * All multi-byte fields are big-endian.
 *
 * <p>Decoders validate every read against the buffer length and throw
 * {@link IllegalArgumentException} on truncated or malformed packets, so
 * callers can reject bad wire input without trapping runtime exceptions.
 */
public final class FilePacket {

    /** F´ FwPacketDescriptorType is a U16 that precedes the file packet. */
    public static final int DESCRIPTOR_LEN = 2;

    /** {@code Fw::ComPacketType::FW_PACKET_FILE}. */
    public static final int FILE_DESCRIPTOR = 0x0003;

    /** Fw::FilePacket::Header is a U8 type + U32 sequenceIndex. */
    public static final int HEADER_LEN = 5;

    /** Fw::FilePacket::Type enum values from FilePacket.hpp. */
    public enum Type {
        START(0), DATA(1), END(2), CANCEL(3);

        public final int value;

        Type(int value) {
            this.value = value;
        }

        public static Type fromValue(int value) {
            for (Type t : values()) {
                if (t.value == value) {
                    return t;
                }
            }
            return null;
        }
    }

    /** Decoded Fw::FilePacket header. */
    public static final class Header {
        public final Type type;
        public final int rawType;
        public final long sequenceIndex;
        /** Offset of the type-specific payload within the source buffer. */
        public final int payloadOffset;

        Header(Type type, int rawType, long sequenceIndex, int payloadOffset) {
            this.type = type;
            this.rawType = rawType;
            this.sequenceIndex = sequenceIndex;
            this.payloadOffset = payloadOffset;
        }
    }

    /** Decoded START packet payload. */
    public static final class StartPayload {
        public final int fileSize;
        public final String sourcePath;
        public final String destinationPath;

        StartPayload(int fileSize, String sourcePath, String destinationPath) {
            this.fileSize = fileSize;
            this.sourcePath = sourcePath;
            this.destinationPath = destinationPath;
        }
    }

    /** Decoded DATA packet payload. */
    public static final class DataPayload {
        public final int byteOffset;
        public final int dataSize;
        /** Offset of the file data within the source buffer. */
        public final int dataStart;

        DataPayload(int byteOffset, int dataSize, int dataStart) {
            this.byteOffset = byteOffset;
            this.dataSize = dataSize;
            this.dataStart = dataStart;
        }
    }

    private FilePacket() {
    }

    // ------------------------------------------------------------------
    // Decoding
    // ------------------------------------------------------------------

    /** Minimum length of a descriptor-prefixed file packet. */
    public static int minimumLength() {
        return DESCRIPTOR_LEN + HEADER_LEN;
    }

    /**
     * Read the U16 packet descriptor at {@code offset}, returning true when
     * it identifies an {@code Fw::FilePacket}.
     */
    public static boolean isFilePacket(byte[] bytes, int offset) {
        require(bytes.length - offset >= DESCRIPTOR_LEN, "packet too short for descriptor");
        int descriptor = ByteBuffer.wrap(bytes).getShort(offset) & 0xFFFF;
        return descriptor == FILE_DESCRIPTOR;
    }

    /**
     * Decode the Fw::FilePacket header from a descriptor-prefixed packet
     * starting at {@code offset}.
     */
    public static Header decodeHeader(byte[] bytes, int offset) {
        require(bytes.length - offset >= minimumLength(), "packet too short for header");
        int innerStart = offset + DESCRIPTOR_LEN;
        int rawType = bytes[innerStart] & 0xFF;
        // U32 on the wire: widen to long so indices >= 2^31 do not wrap
        // negative and trip staleness comparisons on very long transfers.
        long seqIndex = ByteBuffer.wrap(bytes).getInt(innerStart + 1) & 0xFFFFFFFFL;
        return new Header(Type.fromValue(rawType), rawType, seqIndex, innerStart + HEADER_LEN);
    }

    public static StartPayload decodeStart(byte[] bytes, int payloadOffset) {
        require(bytes.length - payloadOffset >= 5, "START payload truncated");
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int fileSize = bb.getInt(payloadOffset);
        int srcLen = bytes[payloadOffset + 4] & 0xFF;
        require(bytes.length - (payloadOffset + 5) >= srcLen + 1,
                "START source path truncated");
        String src = new String(bytes, payloadOffset + 5, srcLen, StandardCharsets.US_ASCII);
        int dstLenOffset = payloadOffset + 5 + srcLen;
        int dstLen = bytes[dstLenOffset] & 0xFF;
        require(bytes.length - (dstLenOffset + 1) >= dstLen,
                "START destination path truncated");
        String dst = new String(bytes, dstLenOffset + 1, dstLen, StandardCharsets.US_ASCII);
        return new StartPayload(fileSize, src, dst);
    }

    public static DataPayload decodeData(byte[] bytes, int payloadOffset) {
        require(bytes.length - payloadOffset >= 6, "DATA payload truncated");
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int byteOffset = bb.getInt(payloadOffset);
        int dataSize = bb.getShort(payloadOffset + 4) & 0xFFFF;
        require(bytes.length - (payloadOffset + 6) >= dataSize,
                "DATA packet shorter than its dataSize field");
        return new DataPayload(byteOffset, dataSize, payloadOffset + 6);
    }

    /** Decode the checksum carried by an END packet. */
    public static int decodeEndChecksum(byte[] bytes, int payloadOffset) {
        require(bytes.length - payloadOffset >= 4, "END payload truncated");
        return ByteBuffer.wrap(bytes).getInt(payloadOffset);
    }

    // ------------------------------------------------------------------
    // Encoding
    // ------------------------------------------------------------------

    /**
     * Encode a START packet:
     * {@code [descriptor U16][type U8][seq U32][fileSize U32][srcLen U8][src][dstLen U8][dst]}.
     */
    public static byte[] encodeStart(int seq, int fileSize, String srcPath, String dstPath) {
        byte[] src = srcPath.getBytes(StandardCharsets.US_ASCII);
        byte[] dst = dstPath.getBytes(StandardCharsets.US_ASCII);
        if (src.length > 255 || dst.length > 255) {
            throw new IllegalArgumentException("Path too long");
        }
        ByteBuffer bb = ByteBuffer.allocate(
                DESCRIPTOR_LEN + HEADER_LEN + 4 + 1 + src.length + 1 + dst.length);
        putHeader(bb, Type.START, seq);
        bb.putInt(fileSize);
        bb.put((byte) src.length).put(src);
        bb.put((byte) dst.length).put(dst);
        return bb.array();
    }

    /**
     * Encode a DATA packet:
     * {@code [descriptor U16][type U8][seq U32][byteOffset U32][dataSize U16][data]}.
     */
    public static byte[] encodeData(int seq, int byteOffset, byte[] source, int srcOff, int len) {
        if (len < 0 || len > 0xFFFF) {
            throw new IllegalArgumentException(
                    "DATA payload length " + len + " outside [0, 65535]");
        }
        ByteBuffer bb = ByteBuffer.allocate(DESCRIPTOR_LEN + HEADER_LEN + 4 + 2 + len);
        putHeader(bb, Type.DATA, seq);
        bb.putInt(byteOffset);
        bb.putShort((short) len);
        bb.put(source, srcOff, len);
        return bb.array();
    }

    /**
     * Encode an END packet:
     * {@code [descriptor U16][type U8][seq U32][checksum U32]}.
     */
    public static byte[] encodeEnd(int seq, int checksum) {
        ByteBuffer bb = ByteBuffer.allocate(DESCRIPTOR_LEN + HEADER_LEN + 4);
        putHeader(bb, Type.END, seq);
        bb.putInt(checksum);
        return bb.array();
    }

    private static void putHeader(ByteBuffer bb, Type type, int seq) {
        bb.putShort((short) FILE_DESCRIPTOR);
        bb.put((byte) type.value);
        bb.putInt(seq);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
