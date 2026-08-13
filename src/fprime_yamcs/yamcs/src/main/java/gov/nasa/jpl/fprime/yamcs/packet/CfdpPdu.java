package gov.nasa.jpl.fprime.yamcs.packet;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Codec for CCSDS File Delivery Protocol PDUs (CCSDS 727.0-B), restricted to
 * the class-1 (unacknowledged) subset needed for F´ file transfer: Metadata,
 * File Data, and EOF PDUs, with small (32-bit) file sizes and the modular
 * checksum ({@link CfdpChecksum}). Encoding uses one-byte entity ids and
 * two-byte transaction sequence numbers; decoding accepts any id/sequence
 * width up to four bytes (F´ CfdpManager transmits one-byte sequences).
 *
 * <p>All multi-byte fields are big-endian. Decoders validate every read
 * against the buffer length and throw {@link IllegalArgumentException} on
 * truncated or malformed PDUs.
 */
public final class CfdpPdu {

    /** Fixed PDU header length with 1-byte entity ids and a 2-byte sequence number. */
    public static final int HEADER_LEN = 8;

    /** File directive codes (CCSDS 727.0-B table 5-4). */
    public static final int DIRECTIVE_EOF = 0x04;
    public static final int DIRECTIVE_FINISHED = 0x05;
    public static final int DIRECTIVE_METADATA = 0x07;

    /** Condition code: no error. */
    public static final int CONDITION_NO_ERROR = 0;
    /** Condition code: cancel request received. */
    public static final int CONDITION_CANCEL_REQUEST = 0x0F;

    /** PDU type per the decoded header. */
    public enum Type {
        FILE_DIRECTIVE, FILE_DATA
    }

    /** Decoded fixed PDU header. */
    public static final class Header {
        public final Type type;
        public final boolean towardSender;
        public final boolean acknowledged;
        public final int dataFieldLength;
        public final int sourceEntityId;
        public final int transactionSeq;
        public final int destinationEntityId;
        /** Offset of the PDU data field within the source buffer. */
        public final int dataOffset;

        Header(Type type, boolean towardSender, boolean acknowledged, int dataFieldLength,
               int sourceEntityId, int transactionSeq, int destinationEntityId, int dataOffset) {
            this.type = type;
            this.towardSender = towardSender;
            this.acknowledged = acknowledged;
            this.dataFieldLength = dataFieldLength;
            this.sourceEntityId = sourceEntityId;
            this.transactionSeq = transactionSeq;
            this.destinationEntityId = destinationEntityId;
            this.dataOffset = dataOffset;
        }
    }

    /** Decoded Metadata PDU. */
    public static final class Metadata {
        public final int fileSize;
        public final String sourceFileName;
        public final String destinationFileName;

        Metadata(int fileSize, String sourceFileName, String destinationFileName) {
            this.fileSize = fileSize;
            this.sourceFileName = sourceFileName;
            this.destinationFileName = destinationFileName;
        }
    }

    /** Decoded File Data PDU payload. */
    public static final class FileData {
        public final int offset;
        public final int dataSize;
        /** Offset of the file data within the source buffer. */
        public final int dataStart;

        FileData(int offset, int dataSize, int dataStart) {
            this.offset = offset;
            this.dataSize = dataSize;
            this.dataStart = dataStart;
        }
    }

    /** Decoded EOF PDU. */
    public static final class Eof {
        public final int conditionCode;
        public final int checksum;
        public final int fileSize;

        Eof(int conditionCode, int checksum, int fileSize) {
            this.conditionCode = conditionCode;
            this.checksum = checksum;
            this.fileSize = fileSize;
        }
    }

    private CfdpPdu() {
    }

    // ------------------------------------------------------------------
    // Decoding
    // ------------------------------------------------------------------

    /** Minimum length of a PDU this codec can decode (1-byte ids and seq). */
    public static int minimumLength() {
        return 4 + 3 + 1;
    }

    /**
     * Decode the PDU header at {@code offset}. Entity ids and transaction
     * sequence numbers of one to four bytes are accepted.
     */
    public static Header decodeHeader(byte[] bytes, int offset) {
        require(bytes.length - offset >= 4, "PDU too short for header");
        int b0 = bytes[offset] & 0xFF;
        int version = (b0 >> 5) & 0x07;
        require(version == 0b001, "unsupported CFDP version " + version);
        Type type = ((b0 >> 4) & 1) == 1 ? Type.FILE_DATA : Type.FILE_DIRECTIVE;
        boolean towardSender = ((b0 >> 3) & 1) == 1;
        boolean acknowledged = ((b0 >> 2) & 1) == 0;
        // Reject the CRC and large-file header options: this codec never
        // encodes them, and decoding them as content would corrupt fields.
        require(((b0 >> 1) & 1) == 0, "unsupported CFDP option: CRC present");
        require((b0 & 1) == 0, "unsupported CFDP option: large file");
        int dataFieldLength = ByteBuffer.wrap(bytes).getShort(offset + 1) & 0xFFFF;
        int b3 = bytes[offset + 3] & 0xFF;
        int entityIdLen = ((b3 >> 4) & 0x07) + 1;
        int seqLen = (b3 & 0x07) + 1;
        require(entityIdLen <= 4 && seqLen <= 4,
                "unsupported entity id/sequence lengths " + entityIdLen + "/" + seqLen);
        int headerLen = 4 + entityIdLen + seqLen + entityIdLen;
        require(bytes.length - offset >= headerLen, "PDU too short for header");
        int src = readUnsigned(bytes, offset + 4, entityIdLen);
        int seq = readUnsigned(bytes, offset + 4 + entityIdLen, seqLen);
        int dst = readUnsigned(bytes, offset + 4 + entityIdLen + seqLen, entityIdLen);
        require(bytes.length - offset - headerLen >= dataFieldLength,
                "PDU shorter than its data field length");
        return new Header(type, towardSender, acknowledged, dataFieldLength,
                src, seq, dst, offset + headerLen);
    }

    /** Read a big-endian unsigned integer of {@code len} (1-4) bytes. */
    private static int readUnsigned(byte[] bytes, int offset, int len) {
        int value = 0;
        for (int i = 0; i < len; i++) {
            value = (value << 8) | (bytes[offset + i] & 0xFF);
        }
        return value;
    }

    /** Read the directive code of a FILE_DIRECTIVE PDU. */
    public static int directiveCode(byte[] bytes, Header header) {
        require(header.type == Type.FILE_DIRECTIVE, "not a file directive PDU");
        require(header.dataFieldLength >= 1, "empty directive PDU");
        return bytes[header.dataOffset] & 0xFF;
    }

    public static Metadata decodeMetadata(byte[] bytes, Header header) {
        require(header.type == Type.FILE_DIRECTIVE, "not a file directive PDU");
        // Confine every read to the header-declared data field so trailing
        // buffer padding is never silently parsed as PDU content.
        int end = header.dataOffset + header.dataFieldLength;
        int p = header.dataOffset + 1; // skip directive code
        require(end - p >= 5, "Metadata PDU truncated");
        // Byte after the directive code: closure requested + checksum type.
        // Only the modular checksum (type 0) is supported; reject others at
        // Metadata time rather than failing at EOF with a confusing mismatch.
        int checksumType = bytes[p] & 0x0F;
        require(checksumType == 0, "unsupported checksum type " + checksumType
                + " (only modular, type 0, is supported)");
        int fileSize = ByteBuffer.wrap(bytes).getInt(p + 1);
        int srcLenOffset = p + 5;
        require(end - srcLenOffset >= 1, "Metadata source LV truncated");
        int srcLen = bytes[srcLenOffset] & 0xFF;
        require(end - (srcLenOffset + 1) >= srcLen + 1,
                "Metadata source file name truncated");
        String src = new String(bytes, srcLenOffset + 1, srcLen, StandardCharsets.US_ASCII);
        int dstLenOffset = srcLenOffset + 1 + srcLen;
        int dstLen = bytes[dstLenOffset] & 0xFF;
        require(end - (dstLenOffset + 1) >= dstLen,
                "Metadata destination file name truncated");
        String dst = new String(bytes, dstLenOffset + 1, dstLen, StandardCharsets.US_ASCII);
        return new Metadata(fileSize, src, dst);
    }

    public static FileData decodeFileData(byte[] bytes, Header header) {
        require(header.type == Type.FILE_DATA, "not a file data PDU");
        require(header.dataFieldLength >= 4, "File Data PDU truncated");
        int offset = ByteBuffer.wrap(bytes).getInt(header.dataOffset);
        int dataSize = header.dataFieldLength - 4;
        return new FileData(offset, dataSize, header.dataOffset + 4);
    }

    public static Eof decodeEof(byte[] bytes, Header header) {
        require(header.type == Type.FILE_DIRECTIVE, "not a file directive PDU");
        int p = header.dataOffset + 1; // skip directive code
        require(header.dataFieldLength >= 1 + 1 + 4 + 4, "EOF PDU truncated");
        int conditionCode = (bytes[p] >> 4) & 0x0F;
        int checksum = ByteBuffer.wrap(bytes).getInt(p + 1);
        int fileSize = ByteBuffer.wrap(bytes).getInt(p + 5);
        return new Eof(conditionCode, checksum, fileSize);
    }

    // ------------------------------------------------------------------
    // Encoding
    // ------------------------------------------------------------------

    /**
     * Encode a class-1 Metadata PDU:
     * {@code [header][0x07][flags/checksum type][fileSize U32][srcLV][dstLV]}.
     */
    public static byte[] encodeMetadata(int sourceEntityId, int destinationEntityId,
                                        int transactionSeq, int fileSize,
                                        String sourceFileName, String destinationFileName) {
        byte[] src = sourceFileName.getBytes(StandardCharsets.US_ASCII);
        byte[] dst = destinationFileName.getBytes(StandardCharsets.US_ASCII);
        if (src.length > 255 || dst.length > 255) {
            throw new IllegalArgumentException("File name too long");
        }
        int dataLen = 1 + 1 + 4 + 1 + src.length + 1 + dst.length;
        ByteBuffer bb = ByteBuffer.allocate(HEADER_LEN + dataLen);
        putHeader(bb, Type.FILE_DIRECTIVE, dataLen,
                sourceEntityId, transactionSeq, destinationEntityId);
        bb.put((byte) DIRECTIVE_METADATA);
        bb.put((byte) 0); // closure not requested, modular checksum (type 0)
        bb.putInt(fileSize);
        bb.put((byte) src.length).put(src);
        bb.put((byte) dst.length).put(dst);
        return bb.array();
    }

    /**
     * Encode a File Data PDU: {@code [header][offset U32][data]}.
     */
    public static byte[] encodeFileData(int sourceEntityId, int destinationEntityId,
                                        int transactionSeq, int offset,
                                        byte[] source, int srcOff, int len) {
        int dataLen = 4 + len;
        ByteBuffer bb = ByteBuffer.allocate(HEADER_LEN + dataLen);
        putHeader(bb, Type.FILE_DATA, dataLen,
                sourceEntityId, transactionSeq, destinationEntityId);
        bb.putInt(offset);
        bb.put(source, srcOff, len);
        return bb.array();
    }

    /**
     * Encode an EOF (no error) PDU:
     * {@code [header][0x04][condition/spare][checksum U32][fileSize U32]}.
     */
    public static byte[] encodeEof(int sourceEntityId, int destinationEntityId,
                                   int transactionSeq, int conditionCode,
                                   int checksum, int fileSize) {
        int dataLen = 1 + 1 + 4 + 4;
        ByteBuffer bb = ByteBuffer.allocate(HEADER_LEN + dataLen);
        putHeader(bb, Type.FILE_DIRECTIVE, dataLen,
                sourceEntityId, transactionSeq, destinationEntityId);
        bb.put((byte) DIRECTIVE_EOF);
        bb.put((byte) ((conditionCode & 0x0F) << 4));
        bb.putInt(checksum);
        bb.putInt(fileSize);
        return bb.array();
    }

    private static void putHeader(ByteBuffer bb, Type type, int dataFieldLength,
                                  int sourceEntityId, int transactionSeq,
                                  int destinationEntityId) {
        if (dataFieldLength > 0xFFFF) {
            throw new IllegalArgumentException(
                    "PDU data field length " + dataFieldLength + " exceeds 65535");
        }
        // version 001 | type | direction: toward receiver | mode: unacknowledged
        // | no CRC | small file
        int b0 = (0b001 << 5) | ((type == Type.FILE_DATA ? 1 : 0) << 4) | (1 << 2);
        bb.put((byte) b0);
        bb.putShort((short) dataFieldLength);
        // no segmentation control, 1-byte entity ids, no segment metadata,
        // 2-byte sequence number
        bb.put((byte) ((0 << 4) | 1));
        if (sourceEntityId < 0 || sourceEntityId > 0xFF
                || destinationEntityId < 0 || destinationEntityId > 0xFF) {
            throw new IllegalArgumentException("entity ids " + sourceEntityId + "/"
                    + destinationEntityId + " outside [0, 255]");
        }
        if (transactionSeq < 0 || transactionSeq > 0xFFFF) {
            throw new IllegalArgumentException(
                    "transaction sequence " + transactionSeq + " outside [0, 65535]");
        }
        bb.put((byte) sourceEntityId);
        bb.putShort((short) transactionSeq);
        bb.put((byte) destinationEntityId);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
