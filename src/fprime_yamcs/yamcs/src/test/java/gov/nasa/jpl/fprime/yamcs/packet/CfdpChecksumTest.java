package gov.nasa.jpl.fprime.yamcs.packet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class CfdpChecksumTest {

    @Test
    public void emptyIsZero() {
        assertEquals(0, CfdpChecksum.of(new byte[0]));
    }

    @Test
    public void singleAlignedWord() {
        assertEquals(0x01020304,
                CfdpChecksum.of(new byte[] { 0x01, 0x02, 0x03, 0x04 }));
    }

    @Test
    public void trailingBytesZeroPadded() {
        // 0x0A0B0000: partial final word is padded with zeros on the right
        assertEquals(0x0A0B0000, CfdpChecksum.of(new byte[] { 0x0A, 0x0B }));
    }

    @Test
    public void wordsSumWithModularOverflow() {
        // 0xFFFFFFFF + 0x00000002 wraps modulo 2^32 to 0x00000001
        byte[] data = new byte[] {
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                0x00, 0x00, 0x00, 0x02,
        };
        assertEquals(1, CfdpChecksum.of(data));
    }

    @Test
    public void multiWordSum() {
        byte[] data = new byte[] {
                0x00, 0x00, 0x00, 0x01,
                0x00, 0x00, 0x00, 0x02,
                0x03, 0x00, 0x00, 0x00,
        };
        assertEquals(0x03000003, CfdpChecksum.of(data));
    }
}
