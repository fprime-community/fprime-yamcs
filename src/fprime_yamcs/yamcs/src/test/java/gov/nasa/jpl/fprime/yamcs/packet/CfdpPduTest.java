package gov.nasa.jpl.fprime.yamcs.packet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class CfdpPduTest {

    @Test
    public void metadataRoundTrip() {
        byte[] pdu = CfdpPdu.encodeMetadata(1, 2, 42, 1234, "src.bin", "/dst/file.bin");
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(CfdpPdu.Type.FILE_DIRECTIVE, h.type);
        assertEquals(1, h.sourceEntityId);
        assertEquals(2, h.destinationEntityId);
        assertEquals(42, h.transactionSeq);
        assertEquals(CfdpPdu.DIRECTIVE_METADATA, CfdpPdu.directiveCode(pdu, h));
        CfdpPdu.Metadata md = CfdpPdu.decodeMetadata(pdu, h);
        assertEquals(1234, md.fileSize);
        assertEquals("src.bin", md.sourceFileName);
        assertEquals("/dst/file.bin", md.destinationFileName);
    }

    @Test
    public void fileDataRoundTrip() {
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        byte[] pdu = CfdpPdu.encodeFileData(1, 2, 7, 100, content, 2, 4);
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(CfdpPdu.Type.FILE_DATA, h.type);
        assertEquals(7, h.transactionSeq);
        CfdpPdu.FileData fd = CfdpPdu.decodeFileData(pdu, h);
        assertEquals(100, fd.offset);
        assertEquals(4, fd.dataSize);
        byte[] data = Arrays.copyOfRange(pdu, fd.dataStart, fd.dataStart + fd.dataSize);
        assertEquals(3, data[0]);
        assertEquals(6, data[3]);
    }

    @Test
    public void eofRoundTrip() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 9, CfdpPdu.CONDITION_NO_ERROR, 0xDEADBEEF, 500);
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(CfdpPdu.DIRECTIVE_EOF, CfdpPdu.directiveCode(pdu, h));
        CfdpPdu.Eof eof = CfdpPdu.decodeEof(pdu, h);
        assertEquals(CfdpPdu.CONDITION_NO_ERROR, eof.conditionCode);
        assertEquals(0xDEADBEEF, eof.checksum);
        assertEquals(500, eof.fileSize);
    }

    @Test
    public void oneByteSequenceNumberAccepted() {
        // F´ CfdpManager encodes 1-byte entity ids AND a 1-byte sequence
        // number; rewrite an encoded EOF into that layout (header shrinks by 1).
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 9, CfdpPdu.CONDITION_NO_ERROR, 0xCAFEBABE, 500);
        byte[] narrow = new byte[pdu.length - 1];
        System.arraycopy(pdu, 0, narrow, 0, 4);
        narrow[3] = 0x00;          // 1-byte ids, 1-byte seq
        narrow[4] = pdu[4];        // src
        narrow[5] = 9;             // seq (1 byte)
        narrow[6] = pdu[7];        // dst
        System.arraycopy(pdu, 8, narrow, 7, pdu.length - 8);

        CfdpPdu.Header h = CfdpPdu.decodeHeader(narrow, 0);
        assertEquals(1, h.sourceEntityId);
        assertEquals(2, h.destinationEntityId);
        assertEquals(9, h.transactionSeq);
        assertEquals(7, h.dataOffset);
        CfdpPdu.Eof eof = CfdpPdu.decodeEof(narrow, h);
        assertEquals(0xCAFEBABE, eof.checksum);
        assertEquals(500, eof.fileSize);
    }

    @Test
    public void oversizedFieldLengthsRejected() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 9, CfdpPdu.CONDITION_NO_ERROR, 0, 0);
        pdu[3] = 0x77; // 8-byte entity ids and sequence number
        assertThrows(IllegalArgumentException.class, () -> CfdpPdu.decodeHeader(pdu, 0));
    }

    @Test
    public void eofCancelConditionCode() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 9, CfdpPdu.CONDITION_CANCEL_REQUEST, 0, 0);
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(CfdpPdu.CONDITION_CANCEL_REQUEST, CfdpPdu.decodeEof(pdu, h).conditionCode);
    }

    @Test
    public void truncatedPdusRejected() {
        byte[] md = CfdpPdu.encodeMetadata(1, 2, 0, 10, "a", "b");
        // Header truncated
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.decodeHeader(Arrays.copyOf(md, CfdpPdu.HEADER_LEN - 1), 0));
        // Data field truncated below the declared data field length
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.decodeHeader(Arrays.copyOf(md, md.length - 1), 0));
        // Metadata payload truncated (patch length field down, cut LV short)
        byte[] cut = Arrays.copyOf(md, CfdpPdu.HEADER_LEN + 7);
        cut[1] = 0;
        cut[2] = 7;
        CfdpPdu.Header h = CfdpPdu.decodeHeader(cut, 0);
        assertThrows(IllegalArgumentException.class, () -> CfdpPdu.decodeMetadata(cut, h));
    }

    @Test
    public void unsupportedVersionRejected() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 0, 0, 0, 0);
        pdu[0] = (byte) ((pdu[0] & 0x1F) | (0b011 << 5));
        assertThrows(IllegalArgumentException.class, () -> CfdpPdu.decodeHeader(pdu, 0));
    }

    @Test
    public void crcFlagRejected() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 0, 0, 0, 0);
        pdu[0] |= 0x02; // CRC present
        assertThrows(IllegalArgumentException.class, () -> CfdpPdu.decodeHeader(pdu, 0));
    }

    @Test
    public void largeFileFlagRejected() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 0, 0, 0, 0);
        pdu[0] |= 0x01; // large file
        assertThrows(IllegalArgumentException.class, () -> CfdpPdu.decodeHeader(pdu, 0));
    }

    @Test
    public void overlongFileNamesRejected() {
        String longName = "x".repeat(256);
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(1, 2, 0, 10, longName, "b"));
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(1, 2, 0, 10, "a", longName));
    }

    @Test
    public void entityIdAndSequenceBoundsEnforced() {
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(-1, 2, 0, 10, "a", "b"));
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(256, 2, 0, 10, "a", "b"));
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(1, 256, 0, 10, "a", "b"));
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(1, 2, -1, 10, "a", "b"));
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(1, 2, 0x10000, 10, "a", "b"));
        // Boundary values round-trip
        byte[] pdu = CfdpPdu.encodeMetadata(255, 255, 0xFFFF, 10, "a", "b");
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(255, h.sourceEntityId);
        assertEquals(255, h.destinationEntityId);
        assertEquals(0xFFFF, h.transactionSeq);
    }

    @Test
    public void metadataDecodingConfinedToDataFieldLength() {
        byte[] md = CfdpPdu.encodeMetadata(1, 2, 0, 10, "abc", "def");
        // Trailing padding after the declared data field must not satisfy
        // the LV reads when the declared length cuts the names short.
        byte[] padded = Arrays.copyOf(md, md.length + 8);
        int declared = ((md[1] & 0xFF) << 8) | (md[2] & 0xFF);
        int shortened = declared - 4;
        padded[1] = (byte) (shortened >> 8);
        padded[2] = (byte) shortened;
        CfdpPdu.Header h = CfdpPdu.decodeHeader(padded, 0);
        assertThrows(IllegalArgumentException.class, () -> CfdpPdu.decodeMetadata(padded, h));
    }

    @Test
    public void encodedPdusAreUnacknowledgedClass1() {
        byte[] pdu = CfdpPdu.encodeMetadata(1, 2, 0, 10, "a", "b");
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertFalse(h.acknowledged);
    }

    @Test
    public void unsupportedChecksumTypeRejectedAtMetadata() {
        byte[] pdu = CfdpPdu.encodeMetadata(1, 2, 0, 10, "a", "b");
        // Flags byte after the directive code carries the checksum type in
        // its low nibble; 15 is the null checksum, which we do not support.
        pdu[CfdpPdu.HEADER_LEN + 1] = (byte) 0x0F;
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.decodeMetadata(pdu, h));
        assertTrue(e.getMessage().contains("checksum type"));
    }
}
