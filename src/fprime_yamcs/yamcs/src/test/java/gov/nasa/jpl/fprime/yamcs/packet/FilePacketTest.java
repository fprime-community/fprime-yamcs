package gov.nasa.jpl.fprime.yamcs.packet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class FilePacketTest {

    @Test
    public void startRoundTrip() {
        byte[] pkt = FilePacket.encodeStart(0, 1234, "/src/file.bin", "/dst/file.bin");
        assertTrue(FilePacket.isFilePacket(pkt, 0));
        FilePacket.Header h = FilePacket.decodeHeader(pkt, 0);
        assertEquals(FilePacket.Type.START, h.type);
        assertEquals(0, h.sequenceIndex);
        FilePacket.StartPayload start = FilePacket.decodeStart(pkt, h.payloadOffset);
        assertEquals(1234, start.fileSize);
        assertEquals("/src/file.bin", start.sourcePath);
        assertEquals("/dst/file.bin", start.destinationPath);
    }

    @Test
    public void dataRoundTrip() {
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        byte[] pkt = FilePacket.encodeData(7, 100, content, 2, 4);
        FilePacket.Header h = FilePacket.decodeHeader(pkt, 0);
        assertEquals(FilePacket.Type.DATA, h.type);
        assertEquals(7, h.sequenceIndex);
        FilePacket.DataPayload data = FilePacket.decodeData(pkt, h.payloadOffset);
        assertEquals(100, data.byteOffset);
        assertEquals(4, data.dataSize);
        assertArrayEquals(new byte[] { 3, 4, 5, 6 },
                Arrays.copyOfRange(pkt, data.dataStart, data.dataStart + data.dataSize));
    }

    @Test
    public void endRoundTrip() {
        byte[] pkt = FilePacket.encodeEnd(9, 0xCAFEBABE);
        FilePacket.Header h = FilePacket.decodeHeader(pkt, 0);
        assertEquals(FilePacket.Type.END, h.type);
        assertEquals(9, h.sequenceIndex);
        assertEquals(0xCAFEBABE, FilePacket.decodeEndChecksum(pkt, h.payloadOffset));
    }

    @Test
    public void startGoldenVector() {
        // [descriptor 0x0003][type 0][seq 1][fileSize 2][srcLen 1]['a'][dstLen 1]['b']
        byte[] pkt = FilePacket.encodeStart(1, 2, "a", "b");
        byte[] expected = new byte[] {
                0x00, 0x03,             // FW_PACKET_FILE descriptor
                0x00,                   // START
                0x00, 0x00, 0x00, 0x01, // sequence index
                0x00, 0x00, 0x00, 0x02, // file size
                0x01, 'a',              // source path
                0x01, 'b',              // destination path
        };
        assertArrayEquals(expected, pkt);
    }

    @Test
    public void nonFileDescriptorRejected() {
        byte[] pkt = new byte[] { 0x00, 0x01, 0, 0, 0, 0, 0 };
        assertFalse(FilePacket.isFilePacket(pkt, 0));
    }

    @Test
    public void truncatedPacketsThrow() {
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.isFilePacket(new byte[] { 0x00 }, 0));
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.decodeHeader(new byte[6], 0));
        // START truncated inside the source path
        byte[] start = FilePacket.encodeStart(0, 10, "abcdef", "ghij");
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.decodeStart(Arrays.copyOf(start, 13),
                        FilePacket.minimumLength()));
        // DATA whose dataSize field exceeds the actual packet length
        byte[] data = FilePacket.encodeData(1, 0, new byte[10], 0, 10);
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.decodeData(Arrays.copyOf(data, data.length - 5),
                        FilePacket.minimumLength()));
        // END without a checksum
        byte[] end = FilePacket.encodeEnd(2, 0);
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.decodeEndChecksum(Arrays.copyOf(end, end.length - 2),
                        FilePacket.minimumLength()));
    }

    @Test
    public void unknownTypeDecodesWithNullEnum() {
        byte[] pkt = new byte[] { 0x00, 0x03, 0x7F, 0, 0, 0, 5 };
        FilePacket.Header h = FilePacket.decodeHeader(pkt, 0);
        assertEquals(null, h.type);
        assertEquals(0x7F, h.rawType);
        assertEquals(5, h.sequenceIndex);
    }

    @Test
    public void sequenceIndexDecodesUnsigned() {
        // A wire sequence index >= 2^31 must not wrap negative.
        byte[] pkt = FilePacket.encodeEnd(-1, 0); // seq 0xFFFFFFFF on the wire
        FilePacket.Header h = FilePacket.decodeHeader(pkt, 0);
        assertEquals(0xFFFFFFFFL, h.sequenceIndex);
    }

    @Test
    public void oversizeDataPayloadRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.encodeData(1, 0, new byte[1], 0, 0x10000));
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.encodeData(1, 0, new byte[1], 0, -1));
    }

    @Test
    public void overlongPathsRejected() {
        String longPath = "x".repeat(256);
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.encodeStart(0, 1, longPath, "/ok"));
        assertThrows(IllegalArgumentException.class,
                () -> FilePacket.encodeStart(0, 1, "/ok", longPath));
    }
}
