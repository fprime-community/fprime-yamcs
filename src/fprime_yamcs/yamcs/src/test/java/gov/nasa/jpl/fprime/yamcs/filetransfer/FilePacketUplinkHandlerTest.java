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
import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

public class FilePacketUplinkHandlerTest {

    private static final int APID = 3;

    private static FprimeFileTransfer transfer(int size) {
        return new FprimeFileTransfer(1, "bucket", "obj.bin", "/dst/obj.bin", size,
                TransferDirection.UPLOAD, "test", false);
    }

    @Test
    public void sendsStartDataEndSequence() {
        byte[] content = new byte[250];
        new Random(2).nextBytes(content);
        List<byte[]> sent = new ArrayList<>();
        RecordingListener listener = new RecordingListener();
        FilePacketUplinkHandler h = new FilePacketUplinkHandler(sent::add, APID, 100, listener);
        FprimeFileTransfer t = transfer(content.length);

        h.run(t, content);

        assertEquals(TransferState.COMPLETED, t.getTransferState());
        assertEquals(content.length, t.getTransferredSize());
        // START + 3 DATA (100+100+50) + END
        assertEquals(5, sent.size());

        // Every packet is a space packet on the file APID with an
        // incrementing sequence count.
        for (int i = 0; i < sent.size(); i++) {
            byte[] pkt = sent.get(i);
            assertEquals(APID, SpacePacket.apid(pkt));
            int seqCtrl = ((pkt[2] & 0xFF) << 8) | (pkt[3] & 0xFF);
            assertEquals(i, seqCtrl & 0x3FFF);
        }

        // Reassemble the inner file packets and verify the content round-trips
        byte[] rebuilt = new byte[content.length];
        for (byte[] pkt : sent) {
            byte[] inner = Arrays.copyOfRange(pkt, SpacePacket.PRIMARY_HEADER_LEN, pkt.length);
            FilePacket.Header header = FilePacket.decodeHeader(inner, 0);
            if (header.type == FilePacket.Type.DATA) {
                FilePacket.DataPayload data = FilePacket.decodeData(inner, header.payloadOffset);
                System.arraycopy(inner, data.dataStart, rebuilt, data.byteOffset, data.dataSize);
            } else if (header.type == FilePacket.Type.END) {
                assertEquals(CfdpChecksum.of(content),
                        FilePacket.decodeEndChecksum(inner, header.payloadOffset));
            }
        }
        assertArrayEquals(content, rebuilt);
    }

    @Test
    public void transportRejectionFailsTransfer() {
        FilePacketUplinkHandler h = new FilePacketUplinkHandler(pkt -> {
            throw new IllegalStateException("Link rejected packet");
        }, APID, 100, new RecordingListener());
        FprimeFileTransfer t = transfer(10);

        h.run(t, new byte[10]);

        assertEquals(TransferState.FAILED, t.getTransferState());
        assertTrue(t.getFailuredReason().contains("Link rejected packet"));
    }

    @Test
    public void interruptionFailsTransferAndRestoresFlag() throws Exception {
        FilePacketUplinkHandler h = new FilePacketUplinkHandler(pkt -> {
            throw new InterruptedException();
        }, APID, 100, new RecordingListener());
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
                () -> new FilePacketUplinkHandler(pkt -> { }, APID, 0, listener));
        assertThrows(IllegalArgumentException.class,
                () -> new FilePacketUplinkHandler(pkt -> { }, APID,
                        FilePacketUplinkHandler.MAX_CHUNK_SIZE + 1, listener));
    }

    @Test
    public void maxChunkSizeProducesEncodableDataPacket() throws Exception {
        // A full chunk at MAX_CHUNK_SIZE must still fit the CCSDS length field.
        List<byte[]> sent = new ArrayList<>();
        FilePacketUplinkHandler h = new FilePacketUplinkHandler(sent::add, APID,
                FilePacketUplinkHandler.MAX_CHUNK_SIZE, new RecordingListener());
        byte[] content = new byte[FilePacketUplinkHandler.MAX_CHUNK_SIZE];
        FprimeFileTransfer t = transfer(content.length);

        h.run(t, content);

        assertEquals(TransferState.COMPLETED, t.getTransferState());
        assertEquals(3, sent.size()); // START + 1 DATA + END
    }
}
