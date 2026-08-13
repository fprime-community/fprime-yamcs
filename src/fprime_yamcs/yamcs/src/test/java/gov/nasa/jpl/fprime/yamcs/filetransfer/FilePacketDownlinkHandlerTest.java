package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpChecksum;
import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;

public class FilePacketDownlinkHandlerTest {

    private FakeBucket bucket;
    private RecordingListener listener;
    private FprimeFileTransfer lastResolved;

    @TempDir
    Path mirrorDir;

    @BeforeEach
    public void setup() {
        bucket = new FakeBucket();
        listener = new RecordingListener();
        lastResolved = null;
    }

    private FilePacketDownlinkHandler handler(int maxFileSize) {
        return handler(maxFileSize, Runnable::run);
    }

    private FilePacketDownlinkHandler handler(int maxFileSize, Executor storageExecutor) {
        return new FilePacketDownlinkHandler(bucket, mirrorDir, maxFileSize,
                (src, dst, size) -> {
                    lastResolved = new FprimeFileTransfer(1, "fake", dst, src, size,
                            TransferDirection.DOWNLOAD, "test", false);
                    return lastResolved;
                }, listener, storageExecutor);
    }

    private static void sendSequence(FilePacketDownlinkHandler h, String dst, byte[] content) {
        h.handleFilePacket(FilePacket.encodeStart(0, content.length, "/src", dst), 0);
        int seq = 1;
        for (int off = 0; off < content.length; off += 100) {
            int len = Math.min(100, content.length - off);
            h.handleFilePacket(FilePacket.encodeData(seq++, off, content, off, len), 0);
        }
        h.handleFilePacket(FilePacket.encodeEnd(seq, CfdpChecksum.of(content)), 0);
    }

    @Test
    public void reassemblesAndStoresFile() throws IOException {
        byte[] content = new byte[350];
        new Random(1).nextBytes(content);
        FilePacketDownlinkHandler h = handler(1024);

        sendSequence(h, "/out/result.bin", content);

        assertArrayEquals(content, bucket.objects.get("out/result.bin"));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
        assertEquals(content.length, lastResolved.getTransferredSize());
        assertArrayEquals(content, Files.readAllBytes(mirrorDir.resolve("out/result.bin")));
    }

    @Test
    public void checksumMismatchFailsTransfer() {
        byte[] content = new byte[10];
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, content.length, "/src", "/f"), 0);
        h.handleFilePacket(FilePacket.encodeData(1, 0, content, 0, content.length), 0);
        h.handleFilePacket(FilePacket.encodeEnd(2, CfdpChecksum.of(content) + 1), 0);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("checksum mismatch"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void oversizeStartIsDroppedWithoutConsumingPending() {
        FilePacketDownlinkHandler h = handler(100);
        h.handleFilePacket(FilePacket.encodeStart(0, 101, "/src", "/f"), 0);
        // A spoofed oversize START must not resolve (and fail) a pending
        // startDownload() transfer; it is dropped outright.
        assertNull(lastResolved);
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void negativeDeclaredSizeIsDropped() {
        FilePacketDownlinkHandler h = handler(100);
        h.handleFilePacket(FilePacket.encodeStart(0, -1, "/src", "/f"), 0);
        assertNull(lastResolved);
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void dataOverflowFailsTransfer() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        byte[] data = new byte[8];
        h.handleFilePacket(FilePacket.encodeData(1, 5, data, 0, data.length), 0);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("overflow"));
    }

    @Test
    public void overflowingByteOffsetFailsTransfer() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        // byteOffset near Integer.MAX_VALUE: int addition would wrap negative
        // and slip past a naive bounds check.
        byte[] pkt = FilePacket.encodeData(1, Integer.MAX_VALUE - 1, new byte[4], 0, 4);
        h.handleFilePacket(pkt, 0);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("overflow"));
    }

    @Test
    public void truncatedDataPacketDoesNotFailInflightTransfer() {
        byte[] content = new byte[10];
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, content.length, "/src", "/f"), 0);
        // DATA header claims 4 bytes but the packet carries none: dropped as
        // undecodable garbage without aborting the healthy transfer.
        byte[] pkt = FilePacket.encodeData(1, 0, new byte[4], 0, 4);
        byte[] truncated = new byte[pkt.length - 4];
        System.arraycopy(pkt, 0, truncated, 0, truncated.length);
        h.handleFilePacket(truncated, 0);
        assertEquals(TransferState.RUNNING, lastResolved.getTransferState());

        h.handleFilePacket(FilePacket.encodeData(2, 0, content, 0, content.length), 0);
        h.handleFilePacket(FilePacket.encodeEnd(3, CfdpChecksum.of(content)), 0);
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
    }

    @Test
    public void stalledTransferExpires() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        h.expireInflight(1, System.currentTimeMillis() + 5);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("stalled"));
    }

    @Test
    public void freshTransferSurvivesExpirySweep() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        h.expireInflight(60_000);
        assertEquals(TransferState.RUNNING, lastResolved.getTransferState());
    }

    @Test
    public void storageBacklogBoundFailsOverflowingTransfer() {
        byte[] content = new byte[10];
        // Executor that never runs its tasks: queued stores never drain.
        FilePacketDownlinkHandler h = handler(1024, task -> { });
        for (int i = 1; i <= FilePacketDownlinkHandler.MAX_PENDING_STORES; i++) {
            sendSequence(h, "/f" + i + ".bin", content);
            assertEquals(TransferState.RUNNING, lastResolved.getTransferState());
        }

        sendSequence(h, "/overflow.bin", content);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("storage backlog"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void traversalDestinationRejected() {
        byte[] content = new byte[4];
        FilePacketDownlinkHandler h = handler(1024);
        sendSequence(h, "/../escape.bin", content);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("unsafe destination path"));
        assertTrue(bucket.objects.isEmpty());
        assertFalse(Files.exists(mirrorDir.getParent().resolve("escape.bin")));
    }

    @Test
    public void cancelFailsInflightTransfer() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        // CANCEL packet: descriptor + type 3 + seq
        byte[] cancel = ByteBuffer.allocate(FilePacket.minimumLength())
                .putShort((short) FilePacket.FILE_DESCRIPTOR)
                .put((byte) FilePacket.Type.CANCEL.value)
                .putInt(1)
                .array();
        h.handleFilePacket(cancel, 0);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("cancelled"));
    }

    @Test
    public void staleCancelIsIgnored() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        h.handleFilePacket(FilePacket.encodeData(1, 0, new byte[4], 0, 4), 0);
        // Replayed CANCEL whose sequence does not advance past the last
        // seen packet must not abort the in-flight transfer.
        byte[] cancel = ByteBuffer.allocate(FilePacket.minimumLength())
                .putShort((short) FilePacket.FILE_DESCRIPTOR)
                .put((byte) FilePacket.Type.CANCEL.value)
                .putInt(1)
                .array();
        h.handleFilePacket(cancel, 0);
        assertEquals(TransferState.RUNNING, lastResolved.getTransferState());
    }

    @Test
    public void newStartSupersedesInflightTransfer() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src1", "/f1"), 0);
        FprimeFileTransfer first = lastResolved;

        byte[] content = new byte[10];
        new Random(7).nextBytes(content);
        sendSequence(h, "/f2", content);

        assertEquals(TransferState.FAILED, first.getTransferState());
        assertTrue(first.getFailuredReason().contains("superseded"));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
        assertArrayEquals(content, bucket.objects.get("f2"));
    }

    @Test
    public void invalidStartDoesNotSupersedeInflightTransfer() {
        byte[] content = new byte[10];
        FilePacketDownlinkHandler h = handler(20);
        h.handleFilePacket(FilePacket.encodeStart(0, content.length, "/src", "/f1"), 0);
        FprimeFileTransfer first = lastResolved;

        // Size-rejected START must not abort the healthy transfer.
        h.handleFilePacket(FilePacket.encodeStart(0, 21, "/src", "/too-big"), 0);
        assertEquals(TransferState.RUNNING, first.getTransferState());

        h.handleFilePacket(FilePacket.encodeData(1, 0, content, 0, content.length), 0);
        h.handleFilePacket(FilePacket.encodeEnd(2, CfdpChecksum.of(content)), 0);
        assertEquals(TransferState.COMPLETED, first.getTransferState());
    }

    @Test
    public void staleOrDuplicateDataPacketIsDropped() {
        byte[] content = new byte[10];
        new Random(3).nextBytes(content);
        byte[] garbage = new byte[10];
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, content.length, "/src", "/f"), 0);
        h.handleFilePacket(FilePacket.encodeData(1, 0, content, 0, content.length), 0);
        // Replayed sequence index carrying different bytes must not poison
        // the already-accepted reassembly.
        h.handleFilePacket(FilePacket.encodeData(1, 0, garbage, 0, garbage.length), 0);
        h.handleFilePacket(FilePacket.encodeEnd(2, CfdpChecksum.of(content)), 0);

        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
        assertArrayEquals(content, bucket.objects.get("f"));
    }

    @Test
    public void incompleteFileFailsAtEnd() {
        byte[] content = new byte[10];
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, content.length, "/src", "/f"), 0);
        // Only half the declared bytes arrive before END.
        h.handleFilePacket(FilePacket.encodeData(1, 0, content, 0, 5), 0);
        h.handleFilePacket(FilePacket.encodeEnd(2, CfdpChecksum.of(content)), 0);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("incomplete"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void bucketWriteFailureFailsTransfer() {
        byte[] content = new byte[10];
        bucket.failPuts = true;
        FilePacketDownlinkHandler h = handler(1024);

        sendSequence(h, "/f", content);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("bucket write failed"));
    }

    @Test
    public void lateStorageCompletionDoesNotResurrectFailedTransfer() {
        byte[] content = new byte[10];
        List<Runnable> deferred = new ArrayList<>();
        FilePacketDownlinkHandler h = handler(1024, deferred::add);

        sendSequence(h, "/f", content);
        // Service shutdown fails the transfer while the write is queued.
        lastResolved.setFailureReason("service stopped");
        lastResolved.setState(TransferState.FAILED);
        listener.stateChanges.clear();

        deferred.forEach(Runnable::run);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(listener.stateChanges.isEmpty(),
                "late completion must not re-announce a terminal transfer");
    }

    @Test
    public void dataAndEndWithNoInflightAreIgnored() {
        FilePacketDownlinkHandler h = handler(1024);
        byte[] data = new byte[4];
        h.handleFilePacket(FilePacket.encodeData(1, 0, data, 0, data.length), 0);
        h.handleFilePacket(FilePacket.encodeEnd(2, 0), 0);
        assertNull(lastResolved);
        assertTrue(bucket.objects.isEmpty());
        assertTrue(listener.stateChanges.isEmpty());
    }

    @Test
    public void completionPublishesVerifierAcks() {
        FilePacketDownlinkHandler h = handler(1024);
        byte[] content = new byte[10];
        sendSequence(h, "/f", content);
        assertTrue(listener.acks.contains(AckStatus.PENDING));
        assertTrue(listener.acks.contains(AckStatus.OK));
    }
}
