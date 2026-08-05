package com.example.myproject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class TlmPacketSplitterTest {

    private static final Map<Long, Integer> SIZES = Map.of(
            100L, 4, // U32
            101L, TlmPacketSplitter.SIZE_STRING,
            102L, 8, // F64
            103L, TlmPacketSplitter.SIZE_UNKNOWN);

    private static byte[] entry(long channelId, int seconds, int microseconds, byte[] value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer header = ByteBuffer.allocate(15);
        header.putInt((int) channelId);
        header.putShort((short) 0); // time base
        header.put((byte) 0); // time context
        header.putInt(seconds);
        header.putInt(microseconds);
        out.write(header.array());
        out.write(value);
        return out.toByteArray();
    }

    private static byte[] packet(byte[]... entries) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(new byte[] { 0x00, 0x01 }); // F Prime packet descriptor: FW_PACKET_TELEM
        for (byte[] e : entries) {
            body.write(e);
        }
        byte[] bodyBytes = body.toByteArray();
        ByteBuffer packet = ByteBuffer.allocate(6 + bodyBytes.length);
        packet.putShort((short) 0x0801); // version 0, TM, APID 1
        packet.putShort((short) 0xC005); // unsegmented, seq count 5
        packet.putShort((short) (bodyBytes.length - 1));
        packet.put(bodyBytes);
        return packet.array();
    }

    private static byte[] stringValue(String s) {
        ByteBuffer buffer = ByteBuffer.allocate(2 + s.length());
        buffer.putShort((short) s.length());
        buffer.put(s.getBytes());
        return buffer.array();
    }

    @Test
    public void testSplitsBatchedEntries() throws Exception {
        TlmPacketSplitter splitter = new TlmPacketSplitter(SIZES, Set.of(102L));
        byte[] u32 = new byte[] { 1, 2, 3, 4 };
        byte[] str = stringValue("hello");
        byte[] f64 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        byte[] packet = packet(
                entry(100, 1000, 250_000, u32),
                entry(101, 1001, 0, str),
                entry(102, 1002, 999_999, f64));

        List<TlmPacketSplitter.SplitEntry> entries = splitter.split(packet);
        assertEquals(3, entries.size());

        assertEquals(100, entries.get(0).channelId);
        assertEquals(101, entries.get(1).channelId);
        assertEquals(102, entries.get(2).channelId);

        // Per-entry generation time: (seconds + 38 leap seconds) * 1000 + usec / 1000
        assertEquals((1000 + 38) * 1000L + 250, entries.get(0).generationTime);
        assertEquals((1001 + 38) * 1000L, entries.get(1).generationTime);
        assertEquals((1002 + 38) * 1000L + 999, entries.get(2).generationTime);

        // doNotArchive follows the channel id, not the first entry of the original packet
        assertFalse(entries.get(0).doNotArchive);
        assertFalse(entries.get(1).doNotArchive);
        assertTrue(entries.get(2).doNotArchive);

        for (TlmPacketSplitter.SplitEntry e : entries) {
            ByteBuffer buffer = ByteBuffer.wrap(e.packet);
            assertEquals(1, buffer.getShort(0) & 0x07FF, "APID preserved");
            assertEquals(3, (buffer.getShort(2) >> 14) & 0x3, "unsegmented sequence flags");
            assertEquals(e.packet.length - 7, buffer.getShort(4) & 0xFFFF, "CCSDS length field");
            assertEquals(1, buffer.getShort(6), "F Prime packet descriptor preserved");
        }

        // Sequence counts increment per synthetic packet
        assertEquals(1, ByteBuffer.wrap(entries.get(0).packet).getShort(2) & 0x3FFF);
        assertEquals(2, ByteBuffer.wrap(entries.get(1).packet).getShort(2) & 0x3FFF);

        // Entry bytes (channel id + time tag + value) are preserved verbatim
        byte[] expected = entry(101, 1001, 0, str);
        byte[] actual = new byte[expected.length];
        System.arraycopy(entries.get(1).packet, 8, actual, 0, expected.length);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testSingleEntryPacket() throws Exception {
        TlmPacketSplitter splitter = new TlmPacketSplitter(SIZES, Set.of());
        List<TlmPacketSplitter.SplitEntry> entries = splitter.split(
                packet(entry(100, 5, 0, new byte[] { 0, 0, 0, 42 })));
        assertEquals(1, entries.size());
        assertEquals(100, entries.get(0).channelId);
    }

    @Test
    public void testUnknownChannelIdRejected() throws Exception {
        TlmPacketSplitter splitter = new TlmPacketSplitter(SIZES, Set.of());
        byte[] packet = packet(entry(999, 0, 0, new byte[] { 0 }));
        assertThrows(TlmPacketSplitter.SplitException.class, () -> splitter.split(packet));
    }

    @Test
    public void testVariableSizeNonStringRejected() throws Exception {
        TlmPacketSplitter splitter = new TlmPacketSplitter(SIZES, Set.of());
        byte[] packet = packet(entry(103, 0, 0, new byte[] { 1, 2, 3 }));
        assertThrows(TlmPacketSplitter.SplitException.class, () -> splitter.split(packet));
    }

    @Test
    public void testTruncatedEntryRejected() throws Exception {
        TlmPacketSplitter splitter = new TlmPacketSplitter(SIZES, Set.of());
        byte[] full = packet(entry(102, 0, 0, new byte[8]));
        byte[] truncated = new byte[full.length - 4];
        System.arraycopy(full, 0, truncated, 0, truncated.length);
        assertThrows(TlmPacketSplitter.SplitException.class, () -> splitter.split(truncated));
    }
}
