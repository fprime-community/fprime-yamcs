package gov.nasa.jpl.fprime.yamcs.packet;

/**
 * CFDP modular checksum (CCSDS 727.0-B section 4.1.2), as used by both
 * {@code Fw::FilePacket} END packets and CFDP EOF PDUs.
 *
 * <p>Direct port of {@code CFDP/Checksum/Checksum.cpp::update} in nasa/fprime.
 */
public final class CfdpChecksum {

    private CfdpChecksum() {
    }

    public static int of(byte[] data) {
        int csum = 0;
        for (int i = 0; i < data.length; i++) {
            int b = data[i] & 0xFF;
            csum += b << (8 * (3 - (i % 4)));
        }
        return csum;
    }
}
