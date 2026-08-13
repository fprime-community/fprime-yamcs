package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.junit.jupiter.api.Test;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpChecksum;
import gov.nasa.jpl.fprime.yamcs.packet.CfdpPdu;

public class CfdpDownlinkHandlerTest {

    private static final int LOCAL = 1;
    private static final int REMOTE = 2;

    private final FakeBucket bucket = new FakeBucket();
    private final RecordingListener listener = new RecordingListener();

    private FprimeFileTransfer lastResolved;

    private CfdpDownlinkHandler handler(int maxFileSize) {
        return handler(maxFileSize, Runnable::run);
    }

    private CfdpDownlinkHandler handler(int maxFileSize, Executor storageExecutor) {
        return new CfdpDownlinkHandler(bucket, null, maxFileSize, LOCAL, REMOTE,
                (src, dst, size) -> {
                    lastResolved = new FprimeFileTransfer(1, "fake", dst, src, size,
                            TransferDirection.DOWNLOAD, "CFDP", false);
                    return lastResolved;
                }, listener, storageExecutor);
    }

    private void feed(CfdpDownlinkHandler h, byte[] pdu) {
        h.handlePdu(pdu, 0);
    }

    private void runTransaction(CfdpDownlinkHandler h, int tx, byte[] content,
                                String dst, int chunk) {
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, tx, content.length, "src.bin", dst));
        for (int off = 0; off < content.length; off += chunk) {
            int len = Math.min(chunk, content.length - off);
            feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, tx, off, content, off, len));
        }
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, tx, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), content.length));
    }

    @Test
    public void reassemblesAndStoresFile() {
        byte[] content = new byte[300];
        new Random(1).nextBytes(content);
        CfdpDownlinkHandler h = handler(1024);

        runTransaction(h, 7, content, "/data/out.bin", 100);

        assertArrayEquals(content, bucket.objects.get("data/out.bin"));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
        assertEquals(content.length, lastResolved.getTransferredSize());
    }

    @Test
    public void completionPublishesVerifierAcks() {
        CfdpDownlinkHandler h = handler(1024);
        byte[] content = new byte[10];
        runTransaction(h, 3, content, "/f.bin", 10);
        // PENDING (at Metadata) must precede the terminal OK (at EOF).
        int pending = listener.acks.indexOf(AckStatus.PENDING);
        int ok = listener.acks.indexOf(AckStatus.OK);
        assertTrue(pending >= 0 && ok >= 0 && pending < ok,
                "expected PENDING then OK, got " + listener.acks);
    }

    @Test
    public void misaddressedPdusAreDropped() {
        CfdpDownlinkHandler h = handler(1024);
        // Wrong source entity
        feed(h, CfdpPdu.encodeMetadata(REMOTE + 1, LOCAL, 1, 10, "s", "/d.bin"));
        // Wrong destination entity
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL + 1, 2, 10, "s", "/d.bin"));
        assertNull(lastResolved, "mis-addressed PDUs must not start a transaction");
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void towardSenderPdusAreDropped() {
        CfdpDownlinkHandler h = handler(1024);
        // An echoed uplink PDU has the right entities but the toward-sender
        // direction bit set; it must not start a downlink transaction.
        byte[] pdu = CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin");
        pdu[0] |= 0x08; // direction: toward file sender
        feed(h, pdu);
        assertNull(lastResolved, "toward-sender PDUs must not start a transaction");
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void newMetadataSupersedesInflightTransaction() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/first.bin"));
        FprimeFileTransfer first = lastResolved;

        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 2, 10, "s", "/second.bin"));
        assertEquals(TransferState.FAILED, first.getTransferState());
        assertTrue(first.getFailuredReason().contains("superseded"));

        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 2, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 2, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
        assertArrayEquals(content, bucket.objects.get("second.bin"));
    }

    @Test
    public void invalidMetadataDoesNotSupersedeInflightTransaction() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(20);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/first.bin"));
        FprimeFileTransfer first = lastResolved;

        // Size-rejected Metadata must not abort the healthy transaction.
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 2, 21, "s", "/too-big.bin"));
        assertEquals(TransferState.RUNNING, first.getTransferState());

        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));
        assertEquals(TransferState.COMPLETED, first.getTransferState());
    }

    @Test
    public void incompleteFileFailsAtEof() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        // Only half the declared bytes arrive before EOF.
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 5));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("incomplete"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void duplicateFileDataDoesNotSatisfyCompleteness() {
        byte[] content = new byte[10];
        new Random(2).nextBytes(content);
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        // Same first half delivered twice; second half lost. Total payload
        // bytes equal the declared size, but coverage does not.
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 5));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 5));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("incomplete"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void overlappingFileDataCompletesOnFullCoverage() {
        byte[] content = new byte[10];
        new Random(3).nextBytes(content);
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        // Overlapping ranges [0,6) and [4,10) cover the whole file.
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 6));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 4, content, 4, 6));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));

        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
        assertArrayEquals(content, bucket.objects.get("d.bin"));
        assertEquals(10, lastResolved.getTransferredSize());
    }

    @Test
    public void bucketWriteFailureFailsTransfer() {
        byte[] content = new byte[10];
        bucket.failPuts = true;
        CfdpDownlinkHandler h = handler(1024);

        runTransaction(h, 1, content, "/d.bin", 10);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("bucket write failed"));
    }

    @Test
    public void rejectedStorageExecutorFailsTransfer() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024, task -> {
            throw new RejectedExecutionException("shutting down");
        });

        runTransaction(h, 1, content, "/d.bin", 10);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("storage executor rejected"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void lateStorageCompletionDoesNotResurrectFailedTransfer() {
        byte[] content = new byte[10];
        List<Runnable> deferred = new ArrayList<>();
        CfdpDownlinkHandler h = handler(1024, deferred::add);

        runTransaction(h, 1, content, "/d.bin", 10);
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
    public void checksumMismatchFailsTransfer() {
        byte[] content = new byte[50];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, content.length, "s", "/d.bin"));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, content.length));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content) + 1, content.length));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("checksum mismatch"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void oversizeMetadataIsDroppedWithoutConsumingPending() {
        CfdpDownlinkHandler h = handler(100);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 101, "s", "/d.bin"));
        // A spoofed oversize Metadata must not resolve (and fail) a pending
        // startDownload() transfer; it is dropped outright.
        assertNull(lastResolved);
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void duplicateMetadataForInflightTransactionIgnored() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        FprimeFileTransfer transfer = lastResolved;
        // Retransmitted Metadata for the same transaction: the reassembly
        // must continue, not restart or fail as superseded.
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        assertSame(transfer, lastResolved);
        assertEquals(TransferState.RUNNING, transfer.getTransferState());

        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));
        assertEquals(TransferState.COMPLETED, transfer.getTransferState());
    }

    @Test
    public void sameSeqMetadataWithDifferentFileSupersedes() {
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        FprimeFileTransfer first = lastResolved;
        // Same 16-bit transaction seq but a different declared size and
        // destination: a genuinely new (wrapped-seq) transaction, not a
        // retransmitted duplicate. It must supersede the previous one.
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 20, "s2", "/other.bin"));
        assertEquals(TransferState.FAILED, first.getTransferState());
        assertTrue(first.getFailuredReason().contains("superseded"));
        assertEquals(TransferState.RUNNING, lastResolved.getTransferState());
    }

    @Test
    public void acknowledgedModePduDropped() {
        CfdpDownlinkHandler h = handler(1024);
        byte[] pdu = CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin");
        pdu[0] &= ~(1 << 2); // clear the mode bit: acknowledged (class-2)
        feed(h, pdu);
        assertNull(lastResolved, "class-2 PDUs must not start a transaction");
    }

    @Test
    public void fileDataOverflowFailsTransfer() {
        CfdpDownlinkHandler h = handler(1024);
        byte[] content = new byte[10];
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        // Offset beyond the declared size
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 8, content, 0, 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("overflow"));
    }

    @Test
    public void negativeOffsetFailsTransfer() {
        CfdpDownlinkHandler h = handler(1024);
        byte[] content = new byte[10];
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, Integer.MIN_VALUE, content, 0, 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
    }

    @Test
    public void cancelConditionCodeFailsTransfer() {
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_CANCEL_REQUEST, 0, 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("cancelled"));
    }

    @Test
    public void eofSizeMismatchFailsTransfer() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 11));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
    }

    @Test
    public void mismatchedTransactionDataDropped() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        // File Data / EOF from another transaction must not disturb tx 1
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 2, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 2, CfdpPdu.CONDITION_NO_ERROR, 0, 10));
        assertEquals(TransferState.RUNNING, lastResolved.getTransferState());

        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
    }

    @Test
    public void traversalDestinationRejected() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/../evil.bin"));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void malformedPduWithNoInflightIsIgnored() {
        CfdpDownlinkHandler h = handler(1024);
        feed(h, new byte[] { 0x20, 0, 0 });
        assertEquals(0, listener.stateChanges.size());
        assertEquals(0, listener.acks.size());
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void malformedPduDoesNotFailInflightTransaction() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        // Garbage on the CFDP APID must not abort the healthy transaction.
        feed(h, new byte[] { 0x20, 0, 0 });
        assertEquals(TransferState.RUNNING, lastResolved.getTransferState());

        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
    }

    @Test
    public void stalledTransactionExpires() {
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        h.expireInflight(1, System.currentTimeMillis() + 5);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("stalled"));
    }

    @Test
    public void freshTransactionSurvivesExpirySweep() {
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        h.expireInflight(60_000);
        assertEquals(TransferState.RUNNING, lastResolved.getTransferState());
    }

    @Test
    public void storageBacklogBoundFailsOverflowingTransfer() {
        byte[] content = new byte[10];
        // Executor that never runs its tasks: queued stores never drain.
        CfdpDownlinkHandler h = handler(1024, task -> { });
        for (int tx = 1; tx <= CfdpDownlinkHandler.MAX_PENDING_STORES; tx++) {
            runTransaction(h, tx, content, "/d" + tx + ".bin", 10);
            assertEquals(TransferState.RUNNING, lastResolved.getTransferState());
        }

        runTransaction(h, 99, content, "/overflow.bin", 10);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("storage backlog"));
        assertTrue(bucket.objects.isEmpty());
    }
}
