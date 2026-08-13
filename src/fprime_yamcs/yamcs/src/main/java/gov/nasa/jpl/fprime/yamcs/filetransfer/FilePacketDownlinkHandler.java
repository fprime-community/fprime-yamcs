package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.buckets.Bucket;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.protobuf.TransferState;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpChecksum;
import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;

/**
 * Reassembles {@code Fw::FilePacket} downlink sequences
 * (START / DATA×N / END, or CANCEL) into complete files, validates the CFDP
 * modular checksum, and stores the result in a YAMCS bucket (optionally
 * mirroring to a local directory).
 *
 * <p>Supports one in-flight transfer at a time, matching the F´
 * {@code Svc::FileDownlink} protocol which is strictly sequential.
 */
public class FilePacketDownlinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilePacketDownlinkHandler.class);

    /**
     * Maximum completed reassemblies awaiting storage. Bounds the memory a
     * fast (or spoofed) TM stream can pin in queued reassembly buffers when
     * bucket writes are slower than downlinks complete.
     */
    static final int MAX_PENDING_STORES = 4;

    /**
     * Supplies the API-level transfer record to attach to a newly started
     * downlink — either a pending {@code startDownload()} transfer keyed by
     * destination path, or a freshly created record for unsolicited
     * downlinks.
     */
    public interface TransferResolver {
        FprimeFileTransfer resolve(String sourcePath, String destinationPath, int fileSize);
    }

    private final Bucket bucket;
    private final Path mirrorDir;
    private final TransferResolver transferResolver;
    private final TransferEventListener listener;
    private final int maxFileSize;
    private final Executor storageExecutor;

    // In-flight downlink reassembly. Null means idle. Guarded by the
    // handler's monitor (all packet entry points are synchronized).
    private Reassembly inflight;
    private long inflightLastActivity;
    private final AtomicInteger pendingStores = new AtomicInteger();

    private static final class Reassembly {
        final String sourcePath;
        final String destinationPath;
        final byte[] buffer;
        final int declaredSize;
        int bytesReceived;
        // Highest FilePacket sequence index accepted so far (START is 0);
        // stale or duplicate DATA packets are rejected against it.
        long lastSequenceIndex;
        final FprimeFileTransfer transfer;

        Reassembly(String src, String dst, int size, FprimeFileTransfer transfer) {
            this.sourcePath = src;
            this.destinationPath = dst;
            this.declaredSize = size;
            this.buffer = new byte[size];
            this.transfer = transfer;
        }
    }

    /**
     * @param bucket           destination bucket for reassembled files
     * @param mirrorDir        optional local mirror directory (null disables)
     * @param maxFileSize      largest START-declared file size accepted, in
     *                         bytes; bounds the reassembly buffer allocation
     *                         so a corrupt/malicious START cannot exhaust
     *                         ground-server memory
     * @param transferResolver supplies the API transfer record for a START
     * @param listener         receives transfer state changes
     * @param storageExecutor  executor on which the completed file is written
     *                         to the bucket/mirror, keeping blocking storage
     *                         I/O off the TM stream subscriber thread
     */
    public FilePacketDownlinkHandler(Bucket bucket, Path mirrorDir, int maxFileSize,
                                     TransferResolver transferResolver,
                                     TransferEventListener listener,
                                     Executor storageExecutor) {
        if (maxFileSize <= 0) {
            throw new IllegalArgumentException("maxFileSize must be positive");
        }
        this.bucket = bucket;
        this.mirrorDir = mirrorDir == null ? null : mirrorDir.normalize();
        this.maxFileSize = maxFileSize;
        this.transferResolver = transferResolver;
        this.listener = listener;
        this.storageExecutor = storageExecutor;
    }

    /**
     * Process one descriptor-prefixed {@code Fw::FilePacket} found at
     * {@code offset} within {@code bytes}.
     */
    public synchronized void handleFilePacket(byte[] bytes, int offset) {
        try {
            FilePacket.Header header = FilePacket.decodeHeader(bytes, offset);
            if (header.type == null) {
                LOG.warn("Unknown FilePacket type {} at seq {}",
                        header.rawType, header.sequenceIndex);
                return;
            }
            switch (header.type) {
                case START:
                    handleStart(bytes, header);
                    break;
                case DATA:
                    handleData(bytes, header);
                    break;
                case END:
                    handleEnd(bytes, header);
                    break;
                case CANCEL:
                    handleCancel(header);
                    break;
            }
        } catch (Exception e) {
            // Drop undecodable packets rather than failing the in-flight
            // transfer: garbage on the file APID must not abort an unrelated
            // healthy transfer. A dead transfer is reclaimed by the
            // expireInflight sweeper.
            LOG.error("Dropping undecodable FilePacket: {}", e.getMessage());
        }
    }

    /**
     * Fail the in-flight transfer if no packet for it has arrived within
     * {@code maxAgeMs}. Called periodically by the owning service so a START
     * never followed by END/CANCEL cannot pin its reassembly buffer forever.
     */
    public void expireInflight(long maxAgeMs) {
        expireInflight(maxAgeMs, System.currentTimeMillis());
    }

    synchronized void expireInflight(long maxAgeMs, long now) {
        if (inflight != null && now - inflightLastActivity > maxAgeMs) {
            LOG.warn("File transfer of {} stalled for over {} ms; failing",
                    inflight.destinationPath, maxAgeMs);
            failInflight("transfer stalled: no packet received within " + maxAgeMs + " ms");
        }
    }

    private void handleStart(byte[] bytes, FilePacket.Header header) {
        // Decode and validate the incoming START *before* superseding the
        // in-flight transfer: a malformed or size-rejected START must not
        // abort an unrelated healthy transfer.
        FilePacket.StartPayload start = FilePacket.decodeStart(bytes, header.payloadOffset);
        LOG.info("File transfer START: seq={} size={} src={} dst={}",
                header.sequenceIndex, start.fileSize,
                ObjectNames.forLog(start.sourcePath), ObjectNames.forLog(start.destinationPath));
        if (start.fileSize < 0 || start.fileSize > maxFileSize) {
            // Drop without consuming any pending startDownload() transfer:
            // a single spoofed START on the TM stream must not be able to
            // fail an operator's pending download. A legitimate oversize
            // downlink is caught by the pending-download timeout sweeper.
            LOG.error("START declares file size {} outside [0, {}]; dropping",
                    start.fileSize, maxFileSize);
            return;
        }

        if (inflight != null) {
            LOG.warn("Got START while transfer in progress; dropping previous");
            failInflight("superseded by new START");
        }
        FprimeFileTransfer transfer = transferResolver.resolve(
                start.sourcePath, start.destinationPath, start.fileSize);
        transfer.setState(TransferState.RUNNING);
        inflight = new Reassembly(start.sourcePath, start.destinationPath, start.fileSize, transfer);
        inflightLastActivity = System.currentTimeMillis();
        listener.stateChanged(transfer);
        listener.verifierAck(transfer, AckStatus.PENDING,
                String.format("receiving %d bytes from %s", start.fileSize, start.sourcePath));
    }

    private void handleData(byte[] bytes, FilePacket.Header header) {
        if (inflight == null) {
            LOG.warn("Got DATA seq={} with no in-flight transfer; dropping", header.sequenceIndex);
            return;
        }
        if (header.sequenceIndex <= inflight.lastSequenceIndex) {
            LOG.warn("Dropping stale/duplicate DATA seq={} (last accepted {})",
                    header.sequenceIndex, inflight.lastSequenceIndex);
            return;
        }
        FilePacket.DataPayload data = FilePacket.decodeData(bytes, header.payloadOffset);
        // Long arithmetic: byteOffset is wire-controlled and int addition
        // could wrap negative and slip past the comparison.
        if (data.byteOffset < 0
                || (long) data.byteOffset + data.dataSize > inflight.declaredSize) {
            LOG.error("DATA packet would overflow file: byteOffset={} dataSize={} declared={}",
                    data.byteOffset, data.dataSize, inflight.declaredSize);
            failInflight("overflow in DATA packet");
            return;
        }
        System.arraycopy(bytes, data.dataStart, inflight.buffer, data.byteOffset, data.dataSize);
        inflight.lastSequenceIndex = header.sequenceIndex;
        inflightLastActivity = System.currentTimeMillis();
        // Clamp: duplicate or overlapping DATA offsets must not inflate the
        // reported progress past the declared file size.
        inflight.bytesReceived = (int) Math.min((long) inflight.declaredSize,
                (long) inflight.bytesReceived + data.dataSize);

        inflight.transfer.setTransferredSize(inflight.bytesReceived);
        listener.stateChanged(inflight.transfer);
    }

    private void handleEnd(byte[] bytes, FilePacket.Header header) {
        if (inflight == null) {
            LOG.warn("Got END seq={} with no in-flight transfer", header.sequenceIndex);
            return;
        }
        if (header.sequenceIndex <= inflight.lastSequenceIndex) {
            LOG.warn("Dropping stale/duplicate END seq={} (last accepted {})",
                    header.sequenceIndex, inflight.lastSequenceIndex);
            return;
        }
        int receivedChecksum = FilePacket.decodeEndChecksum(bytes, header.payloadOffset);
        if (inflight.bytesReceived != inflight.declaredSize) {
            failInflight(String.format(
                    "incomplete file: received %d of %d declared bytes",
                    inflight.bytesReceived, inflight.declaredSize));
            return;
        }
        int computed = CfdpChecksum.of(inflight.buffer);
        if (computed != receivedChecksum) {
            LOG.error("Checksum mismatch on transfer {}: received=0x{} computed=0x{}",
                    inflight.destinationPath,
                    Integer.toHexString(receivedChecksum),
                    Integer.toHexString(computed));
            failInflight(String.format(
                    "checksum mismatch: expected 0x%08x got 0x%08x",
                    receivedChecksum, computed));
            return;
        }

        String objectName;
        try {
            objectName = ObjectNames.sanitize(inflight.destinationPath);
        } catch (IllegalArgumentException e) {
            LOG.error("Rejecting unsafe destination path '{}': {}",
                    ObjectNames.forLog(inflight.destinationPath), e.getMessage());
            failInflight("unsafe destination path: " + e.getMessage());
            return;
        }
        // Hand storage off so blocking bucket/mirror I/O never stalls the TM
        // stream subscriber thread. Clear the in-flight slot first: the
        // reassembly is complete and the next START may arrive immediately.
        // Bound the storage backlog so a fast TM stream cannot pin unbounded
        // memory in queued reassembly buffers.
        // Atomic reserve-then-rollback (same pattern as submitUplink): the
        // bound must not depend on callers holding the handler monitor.
        if (pendingStores.incrementAndGet() > MAX_PENDING_STORES) {
            pendingStores.decrementAndGet();
            failInflight("storage backlog: " + MAX_PENDING_STORES
                    + " completed transfers already awaiting bucket writes");
            return;
        }
        Reassembly completed = inflight;
        inflight = null;
        try {
            storageExecutor.execute(() -> {
                try {
                    store(completed, objectName);
                } finally {
                    pendingStores.decrementAndGet();
                }
            });
        } catch (RejectedExecutionException e) {
            pendingStores.decrementAndGet();
            String reason = "storage executor rejected write: " + e.getMessage();
            completed.transfer.fail(reason);
            listener.stateChanged(completed.transfer);
            listener.verifierAck(completed.transfer, AckStatus.NOK, reason);
        }
    }

    private void store(Reassembly completed, String objectName) {
        try {
            bucket.putObjectAsync(objectName, "application/octet-stream",
                    Map.of(), completed.buffer).join();
            LOG.info("File transfer COMPLETE: {} ({} bytes) -> bucket {}",
                    objectName, completed.bytesReceived, bucket.getName());

            DownlinkMirror.write(mirrorDir, objectName, completed.buffer);

            completed.transfer.setTransferredSize(completed.bytesReceived);
            if (!completed.transfer.setState(TransferState.COMPLETED)) {
                // Already terminal — e.g. failed by service shutdown while
                // this write was in flight. Do not resurrect it.
                return;
            }
            listener.stateChanged(completed.transfer);
            listener.verifierAck(completed.transfer, AckStatus.OK,
                    String.format("delivered %d bytes to bucket %s/%s",
                            completed.bytesReceived, bucket.getName(), objectName));
        } catch (Exception e) {
            LOG.error("Failed to store file in bucket", e);
            String reason = "bucket write failed: " + e.getMessage();
            completed.transfer.fail(reason);
            listener.stateChanged(completed.transfer);
            listener.verifierAck(completed.transfer, AckStatus.NOK, reason);
        }
    }

    private void handleCancel(FilePacket.Header header) {
        if (inflight != null) {
            // Same staleness gate as DATA/END: a replayed CANCEL from an
            // earlier transfer must not abort the current one.
            if (header.sequenceIndex <= inflight.lastSequenceIndex) {
                LOG.warn("Ignoring stale CANCEL seq={} (last seen {})",
                        header.sequenceIndex, inflight.lastSequenceIndex);
                return;
            }
            LOG.warn("File transfer CANCELLED at seq {} (was: {})",
                    header.sequenceIndex, inflight.destinationPath);
            failInflight("cancelled by spacecraft");
        } else {
            LOG.warn("Got CANCEL seq={} with no in-flight transfer", header.sequenceIndex);
        }
    }

    /**
     * Mark the in-flight downlink as failed, push the failure to its API
     * transfer, and clear the in-flight state. Used by every error path in
     * the downlink state machine.
     */
    private void failInflight(String reason) {
        if (inflight == null) {
            return;
        }
        inflight.transfer.fail(reason);
        listener.stateChanged(inflight.transfer);
        listener.verifierAck(inflight.transfer, AckStatus.NOK, reason);
        inflight = null;
    }
}
