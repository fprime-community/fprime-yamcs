package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpChecksum;
import gov.nasa.jpl.fprime.yamcs.packet.CfdpPdu;
import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

public class CfdpUplinkHandlerTest {

    private static final int APID = 5;

    private static FprimeFileTransfer transfer(int size) {
        return new FprimeFileTransfer(1, "bucket", "obj.bin", "/dst/obj.bin", size,
                TransferDirection.UPLOAD, "CFDP", false);
    }

    @Test
    public void sendsMetadataFileDataEofSequence() {
        byte[] content = new byte[250];
        new Random(3).nextBytes(content);
        List<byte[]> sent = new ArrayList<>();
        CfdpUplinkHandler h = new CfdpUplinkHandler(sent::add, APID, 100, 1, 2,
                new RecordingListener());
        FprimeFileTransfer t = transfer(content.length);

        h.run(t, content);

        assertEquals(TransferState.COMPLETED, t.getTransferState());
        assertEquals(content.length, t.getTransferredSize());
        // Metadata + 3 File Data (100+100+50) + EOF
        assertEquals(5, sent.size());

        byte[] rebuilt = new byte[content.length];
        for (int i = 0; i < sent.size(); i++) {
            byte[] pkt = sent.get(i);
            assertEquals(APID, SpacePacket.apid(pkt));
            int seqCtrl = ((pkt[2] & 0xFF) << 8) | (pkt[3] & 0xFF);
            assertEquals(i, seqCtrl & 0x3FFF);

            // Each packet carries the F´ FW_PACKET_FILE descriptor, then the PDU.
            assertTrue(FilePacket.isFilePacket(pkt, SpacePacket.PRIMARY_HEADER_LEN));
            byte[] pdu = Arrays.copyOfRange(pkt,
                    SpacePacket.PRIMARY_HEADER_LEN + FilePacket.DESCRIPTOR_LEN, pkt.length);
            CfdpPdu.Header header = CfdpPdu.decodeHeader(pdu, 0);
            assertEquals(1, header.sourceEntityId);
            assertEquals(2, header.destinationEntityId);
            if (header.type == CfdpPdu.Type.FILE_DATA) {
                CfdpPdu.FileData fd = CfdpPdu.decodeFileData(pdu, header);
                System.arraycopy(pdu, fd.dataStart, rebuilt, fd.offset, fd.dataSize);
            } else if (CfdpPdu.directiveCode(pdu, header) == CfdpPdu.DIRECTIVE_METADATA) {
                CfdpPdu.Metadata md = CfdpPdu.decodeMetadata(pdu, header);
                assertEquals(content.length, md.fileSize);
                assertEquals("obj.bin", md.sourceFileName);
                assertEquals("/dst/obj.bin", md.destinationFileName);
            } else {
                CfdpPdu.Eof eof = CfdpPdu.decodeEof(pdu, header);
                assertEquals(CfdpChecksum.of(content), eof.checksum);
                assertEquals(content.length, eof.fileSize);
            }
        }
        assertArrayEquals(content, rebuilt);
    }

    @Test
    public void transactionSequenceIncrementsPerTransfer() {
        List<byte[]> sent = new ArrayList<>();
        CfdpUplinkHandler h = new CfdpUplinkHandler(sent::add, APID, 100, 1, 2,
                new RecordingListener());
        h.run(transfer(10), new byte[10]);
        int firstTx = txOf(sent.get(0));
        sent.clear();
        h.run(transfer(10), new byte[10]);
        // The counter is 16-bit and time-seeded, so mask for the wrap case.
        assertEquals((firstTx + 1) & 0xFFFF, txOf(sent.get(0)));
    }

    private static int txOf(byte[] pkt) {
        byte[] pdu = Arrays.copyOfRange(pkt,
                SpacePacket.PRIMARY_HEADER_LEN + FilePacket.DESCRIPTOR_LEN, pkt.length);
        return CfdpPdu.decodeHeader(pdu, 0).transactionSeq;
    }

    @Test
    public void transportRejectionFailsTransfer() {
        CfdpUplinkHandler h = new CfdpUplinkHandler(pkt -> {
            throw new IllegalStateException("Link rejected packet");
        }, APID, 100, 1, 2, new RecordingListener());
        FprimeFileTransfer t = transfer(10);

        h.run(t, new byte[10]);

        assertEquals(TransferState.FAILED, t.getTransferState());
        assertTrue(t.getFailuredReason().contains("Link rejected packet"));
    }

    @Test
    public void interruptionFailsTransferAndRestoresFlag() throws Exception {
        CfdpUplinkHandler h = new CfdpUplinkHandler(pkt -> {
            throw new InterruptedException();
        }, APID, 100, 1, 2, new RecordingListener());
        FprimeFileTransfer t = transfer(10);

        AtomicBoolean flagRestored = new AtomicBoolean();
        Thread worker = new Thread(() -> {
            h.run(t, new byte[10]);
            flagRestored.set(Thread.currentThread().isInterrupted());
        });
        worker.start();
        worker.join(5000);

        assertFalse(worker.isAlive());
        assertTrue(flagRestored.get());
        assertEquals(TransferState.FAILED, t.getTransferState());
        assertEquals("interrupted", t.getFailuredReason());
    }

    @Test
    public void chunkSizeBoundsEnforced() {
        RecordingListener listener = new RecordingListener();
        assertThrows(IllegalArgumentException.class,
                () -> new CfdpUplinkHandler(pkt -> { }, APID, 0, 1, 2, listener));
        assertThrows(IllegalArgumentException.class,
                () -> new CfdpUplinkHandler(pkt -> { }, APID,
                        CfdpUplinkHandler.MAX_CHUNK_SIZE + 1, 1, 2, listener));
    }

    @Test
    public void maxChunkSizeProducesEncodablePdu() {
        List<byte[]> sent = new ArrayList<>();
        CfdpUplinkHandler h = new CfdpUplinkHandler(sent::add, APID,
                CfdpUplinkHandler.MAX_CHUNK_SIZE, 1, 2, new RecordingListener());
        byte[] content = new byte[CfdpUplinkHandler.MAX_CHUNK_SIZE];
        FprimeFileTransfer t = transfer(content.length);

        h.run(t, content);

        assertEquals(TransferState.COMPLETED, t.getTransferState());
        assertEquals(3, sent.size()); // Metadata + 1 File Data + EOF
    }
}
