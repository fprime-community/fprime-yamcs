package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.yamcs.InitException;
import org.yamcs.YConfiguration;
import org.yamcs.filetransfer.InvalidRequestException;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpPdu;
import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

/**
 * TM-stream dispatch filters: APID matching, minimum-length short-circuit,
 * descriptor check, and trimming trailing frame padding to the
 * CCSDS-declared length before bytes reach the downlink handlers.
 */
public class ServicePacketExtractionTest {

    private static final int APID = 5;

    private static byte[] cfdpSpacePacket() {
        byte[] pdu = CfdpPdu.encodeMetadata(2, 1, 7, 10, "s", "/d.bin");
        return SpacePacket.wrapTelecommand(pdu, APID, 0);
    }

    private static byte[] filePacketSpacePacket() {
        byte[] pkt = FilePacket.encodeStart(0, 10, "/src", "/f");
        return SpacePacket.wrapTelecommand(pkt, APID, 0);
    }

    private static byte[] withPadding(byte[] packet, int padding) {
        return Arrays.copyOf(packet, packet.length + padding);
    }

    @Test
    public void cfdpPacketOnConfiguredApidPassesThrough() {
        byte[] packet = cfdpSpacePacket();
        assertSame(packet, CfdpFileTransferService.extractCfdpPacket(packet, APID));
    }

    @Test
    public void cfdpPaddingTrimmedToDeclaredLength() {
        byte[] packet = cfdpSpacePacket();
        byte[] padded = withPadding(packet, 32);
        assertArrayEquals(packet, CfdpFileTransferService.extractCfdpPacket(padded, APID));
    }

    @Test
    public void cfdpWrongApidAndShortPacketsDropped() {
        byte[] packet = cfdpSpacePacket();
        assertNull(CfdpFileTransferService.extractCfdpPacket(packet, APID + 1));
        assertNull(CfdpFileTransferService.extractCfdpPacket(new byte[4], APID));
    }

    @Test
    public void pduOffsetSkipsFileDescriptor() {
        // F´ CfdpManager frames PDUs behind the FW_PACKET_FILE descriptor.
        byte[] pdu = CfdpPdu.encodeMetadata(2, 1, 7, 10, "s", "/d.bin");
        byte[] framed = new byte[FilePacket.DESCRIPTOR_LEN + pdu.length];
        framed[0] = (byte) (FilePacket.FILE_DESCRIPTOR >> 8);
        framed[1] = (byte) FilePacket.FILE_DESCRIPTOR;
        System.arraycopy(pdu, 0, framed, FilePacket.DESCRIPTOR_LEN, pdu.length);
        byte[] packet = SpacePacket.wrapTelecommand(framed, APID, 0);

        int off = CfdpFileTransferService.pduOffset(packet);
        assertEquals(SpacePacket.PRIMARY_HEADER_LEN + FilePacket.DESCRIPTOR_LEN, off);
        assertEquals(7, CfdpPdu.decodeHeader(packet, off).transactionSeq);
    }

    @Test
    public void pduOffsetAcceptsRawPdu() {
        byte[] packet = cfdpSpacePacket();
        int off = CfdpFileTransferService.pduOffset(packet);
        assertEquals(SpacePacket.PRIMARY_HEADER_LEN, off);
        assertEquals(7, CfdpPdu.decodeHeader(packet, off).transactionSeq);
    }

    @Test
    public void filePacketOnConfiguredApidPassesThrough() {
        byte[] packet = filePacketSpacePacket();
        assertSame(packet, FprimeFilePacketService.extractFilePacket(packet, APID));
    }

    @Test
    public void filePacketPaddingTrimmedToDeclaredLength() {
        byte[] packet = filePacketSpacePacket();
        byte[] padded = withPadding(packet, 32);
        assertArrayEquals(packet, FprimeFilePacketService.extractFilePacket(padded, APID));
    }

    @Test
    public void filePacketWrongApidShortOrBadDescriptorDropped() {
        byte[] packet = filePacketSpacePacket();
        assertNull(FprimeFilePacketService.extractFilePacket(packet, APID + 1));
        assertNull(FprimeFilePacketService.extractFilePacket(new byte[4], APID));

        byte[] badDescriptor = packet.clone();
        badDescriptor[SpacePacket.PRIMARY_HEADER_LEN] = 0x7F;
        badDescriptor[SpacePacket.PRIMARY_HEADER_LEN + 1] = 0x7F;
        assertNull(FprimeFilePacketService.extractFilePacket(badDescriptor, APID));
    }

    // ------------------------------------------------------------------
    // CFDP service startup validation and unsupported-operation contract
    // ------------------------------------------------------------------

    private static void initService(Map<String, Object> args) throws InitException {
        new CfdpFileTransferService().init("test", "CfdpFileTransferService",
                YConfiguration.wrap(args));
    }

    @Test
    public void initRejectsOutOfRangeApidAndEntityIds() {
        assertThrows(InitException.class,
                () -> initService(Map.of("cfdpApid", SpacePacket.MAX_APID + 1)));
        assertThrows(InitException.class, () -> initService(Map.of("localEntityId", 256)));
        assertThrows(InitException.class, () -> initService(Map.of("remoteEntityId", -1)));
    }

    @Test
    public void unsupportedOperationsThrow() throws InitException {
        CfdpFileTransferService svc = new CfdpFileTransferService();
        svc.init("test", "CfdpFileTransferService", YConfiguration.wrap(Map.of()));
        assertThrows(UnsupportedOperationException.class, () -> svc.pause(null));
        assertThrows(UnsupportedOperationException.class, () -> svc.resume(null));
        assertThrows(UnsupportedOperationException.class, () -> svc.cancel(null));
        assertThrows(InvalidRequestException.class,
                () -> svc.fetchFileList("l", "r", "/", Map.of()));
        assertThrows(InvalidRequestException.class,
                () -> svc.getFileList("l", "r", "/", Map.of()));
        assertEquals(java.util.Set.of(), svc.getRemoteFileListMonitors());
    }
}
