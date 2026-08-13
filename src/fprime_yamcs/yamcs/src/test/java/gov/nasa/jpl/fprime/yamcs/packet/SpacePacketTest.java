package gov.nasa.jpl.fprime.yamcs.packet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class SpacePacketTest {

    @Test
    public void wrapTelecommandHeader() {
        byte[] payload = new byte[] { 9, 8, 7 };
        byte[] pkt = SpacePacket.wrapTelecommand(payload, 3, 5);
        assertEquals(SpacePacket.PRIMARY_HEADER_LEN + payload.length, pkt.length);
        // Word 0: version 0, type TC (1), no secondary header, APID 3
        assertEquals(0x10, pkt[0] & 0xFF);
        assertEquals(0x03, pkt[1] & 0xFF);
        // Word 1: standalone sequence flags, count 5
        assertEquals(0xC0, pkt[2] & 0xFF);
        assertEquals(0x05, pkt[3] & 0xFF);
        // Word 2: data length minus one
        assertEquals(0x00, pkt[4] & 0xFF);
        assertEquals(payload.length - 1, pkt[5] & 0xFF);
        assertArrayEquals(payload, Arrays.copyOfRange(pkt, 6, pkt.length));
        assertEquals(3, SpacePacket.apid(pkt));
    }

    @Test
    public void sequenceCountMaskedTo14Bits() {
        byte[] pkt = SpacePacket.wrapTelecommand(new byte[] { 1 }, 1, 0x7FFF);
        // 0b11 flags | (0x7FFF & 0x3FFF)
        assertEquals(0xFF, pkt[2] & 0xFF);
        assertEquals(0xFF, pkt[3] & 0xFF);
    }

    @Test
    public void payloadBoundsEnforced() {
        assertThrows(IllegalArgumentException.class,
                () -> SpacePacket.wrapTelecommand(new byte[0], 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> SpacePacket.wrapTelecommand(
                        new byte[SpacePacket.MAX_PAYLOAD_LEN + 1], 1, 0));
        // Boundary case: exactly the maximum encodes length field 0xFFFF
        byte[] pkt = SpacePacket.wrapTelecommand(new byte[SpacePacket.MAX_PAYLOAD_LEN], 1, 0);
        assertEquals(0xFF, pkt[4] & 0xFF);
        assertEquals(0xFF, pkt[5] & 0xFF);
    }

    @Test
    public void apidBoundsEnforced() {
        assertThrows(IllegalArgumentException.class,
                () -> SpacePacket.wrapTelecommand(new byte[] { 1 }, -1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> SpacePacket.wrapTelecommand(new byte[] { 1 }, SpacePacket.MAX_APID + 1, 0));
        byte[] pkt = SpacePacket.wrapTelecommand(new byte[] { 1 }, SpacePacket.MAX_APID, 0);
        assertEquals(SpacePacket.MAX_APID, SpacePacket.apid(pkt));
    }

    @Test
    public void headerAccessorsRejectShortPackets() {
        byte[] shortPkt = new byte[SpacePacket.PRIMARY_HEADER_LEN - 1];
        assertThrows(IllegalArgumentException.class, () -> SpacePacket.apid(shortPkt));
        assertThrows(IllegalArgumentException.class,
                () -> SpacePacket.packetIdAndSequence(shortPkt));
        assertThrows(IllegalArgumentException.class,
                () -> SpacePacket.declaredLength(shortPkt));
    }

    @Test
    public void declaredLengthUsesMinusOneConvention() {
        byte[] pkt = SpacePacket.wrapTelecommand(new byte[] { 9, 8, 7 }, 3, 5);
        assertEquals(pkt.length, SpacePacket.declaredLength(pkt));
        // Trailing padding is not counted
        byte[] padded = Arrays.copyOf(pkt, pkt.length + 4);
        assertEquals(pkt.length, SpacePacket.declaredLength(padded));
    }
}
