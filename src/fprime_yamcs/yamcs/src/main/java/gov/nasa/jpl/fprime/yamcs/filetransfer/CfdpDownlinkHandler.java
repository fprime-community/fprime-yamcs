package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.buckets.Bucket;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.protobuf.TransferState;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpChecksum;
import gov.nasa.jpl.fprime.yamcs.packet.CfdpPdu;

/**
 * Reassembles class-1 CFDP downlink transactions (Metadata / File Data×N /
 * EOF) into complete files, validates the CFDP modular checksum, and stores
 * the result in a YAMCS bucket (optionally mirroring to a local directory).
 *
 * <p>Supports one in-flight transaction at a time. An EOF with a non-zero
 * condition code cancels the in-flight transaction.
 */
public class CfdpDownlinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CfdpDownlinkHandler.class);

    /**
     * Maximum completed reassemblies awaiting storage. Bounds the memory a
     * fast (or spoofed) TM stream can pin in queued reassembly buffers when
     * bucket writes are slower than downlinks complete.
     */
    static final int MAX_PENDING_STORES = 4;

    /** Supplies the API-level transfer record for a new downlink transaction. */
    public interface TransferResolver {
        FprimeFileTransfer resolve(String sourceFileName, String destinationFileName,
                                   int fileSize);
    }

    private final Bucket bucket;
    private final Path mirrorDir;
    private final int maxFileSize;
    private final int localEntityId;
    private final int remoteEntityId;
    private final TransferResolver transferResolver;
    private final TransferEventListener listener;
    private final Executor storageExecutor;

    // In-flight transaction reassembly. Null means idle. Guarded by the
    // handler's monitor (all packet entry points are synchronized).
    private Reassembly inflight;
    private long inflightLastActivity;
    private final AtomicInteger pendingStores = new AtomicInteger();

    private static final class Reassembly {
        final int transactionSeq;
        final String destinationFileName;
        final byte[] buffer;
        final int declaredSize;
        int bytesReceived;
        final FprimeFileTransfer transfer;
        // Merged [start, end) byte ranges covered so far. CFDP File Data
        // PDUs carry no sequence index, so duplicates/overlaps must be
        // deduplicated by extent or the EOF completeness check could pass
        // with a gap in the buffer.
        private final TreeMap<Integer, Integer> extents = new TreeMap<>();

        Reassembly(int transactionSeq, String dst, int size, FprimeFileTransfer transfer) {
            this.transactionSeq = transactionSeq;
            this.destinationFileName = dst;
            this.declaredSize = size;
            this.buffer = new byte[size];
            this.transfer = transfer;
        }

        /** Record [start, end) as received; bytesReceived counts coverage. */
        void addExtent(int start, int end) {
            if (end <= start) {
                return;
            }
            Map.Entry<Integer, Integer> floor = extents.floorEntry(start);
            int newStart = (floor != null && floor.getValue() >= start)
                    ? floor.getKey() : start;
            int newEnd = end;
            int merged = 0;
            var overlapping = extents.subMap(newStart, true, end, true);
            for (Map.Entry<Integer, Integer> e : overlapping.entrySet()) {
                merged += e.getValue() - e.getKey();
                newEnd = Math.max(newEnd, e.getValue());
            }
            overlapping.clear();
            extents.put(newStart, newEnd);
            bytesReceived += (newEnd - newStart) - merged;
        }
    }

    /**
     * @param bucket           destination bucket for reassembled files
     * @param mirrorDir        optional local mirror directory (null disables)
     * @param maxFileSize      largest Metadata-declared file size accepted,
     *                         in bytes; bounds the reassembly buffer
     * @param localEntityId    this ground entity's CFDP id; PDUs not
     *                         addressed to it are dropped
     * @param remoteEntityId   the spacecraft entity id PDUs must come from
     * @param transferResolver supplies the API transfer record for a Metadata
     * @param listener         receives transfer state changes
     * @param storageExecutor  executor on which the completed file is written
     *                         to the bucket/mirror, keeping blocking storage
     *                         I/O off the TM stream subscriber thread
     */
    public CfdpDownlinkHandler(Bucket bucket, Path mirrorDir, int maxFileSize,
                               int localEntityId, int remoteEntityId,
                               TransferResolver transferResolver,
                               TransferEventListener listener,
                               Executor storageExecutor) {
        if (maxFileSize <= 0) {
            throw new IllegalArgumentException("maxFileSize must be positive");
        }
        this.bucket = bucket;
        this.mirrorDir = mirrorDir == null ? null : mirrorDir.normalize();
        this.maxFileSize = maxFileSize;
        this.localEntityId = localEntityId;
        this.remoteEntityId = remoteEntityId;
        this.transferResolver = transferResolver;
        this.listener = listener;
        this.storageExecutor = storageExecutor;
    }

    /** Process one CFDP PDU found at {@code offset} within {@code bytes}. */
    public synchronized void handlePdu(byte[] bytes, int offset) {
        try {
            CfdpPdu.Header header = CfdpPdu.decodeHeader(bytes, offset);
            // Only PDUs addressed spacecraft -> this ground entity belong to
            // this downlink; mis-routed traffic (an echoed uplink PDU, or a
            // second entity sharing the APID) must not touch the in-flight
            // reassembly.
            if (header.towardSender
                    || header.sourceEntityId != remoteEntityId
                    || header.destinationEntityId != localEntityId) {
                LOG.warn("Dropping mis-addressed CFDP PDU: entities {}->{} towardSender={}"
                        + " (expected {}->{})", header.sourceEntityId,
                        header.destinationEntityId, header.towardSender,
                        remoteEntityId, localEntityId);
                return;
            }
            // Class-2 (acknowledged) transactions expect ACK/Finished PDUs
            // this class-1 receiver never sends; drop rather than silently
            // reassembling under the wrong semantics.
            if (header.acknowledged) {
                LOG.warn("Dropping acknowledged-mode (class-2) CFDP PDU; "
                        + "only unacknowledged (class-1) transactions are supported");
                return;
            }
            if (header.type == CfdpPdu.Type.FILE_DATA) {
                handleFileData(bytes, header);
                return;
            }
            int directive = CfdpPdu.directiveCode(bytes, header);
            switch (directive) {
                case CfdpPdu.DIRECTIVE_METADATA:
                    handleMetadata(bytes, header);
                    break;
                case CfdpPdu.DIRECTIVE_EOF:
                    handleEof(bytes, header);
                    break;
                default:
                    LOG.warn("Ignoring CFDP directive 0x{} on tx {}",
                            Integer.toHexString(directive), header.transactionSeq);
            }
        } catch (Exception e) {
            // Drop undecodable PDUs rather than failing the in-flight
            // transaction: garbage on the CFDP APID must not abort an
            // unrelated healthy transfer. A dead transaction is reclaimed
            // by the expireInflight sweeper.
            LOG.error("Dropping undecodable CFDP PDU: {}", e.getMessage());
        }
    }

    /**
     * Fail the in-flight transaction if no PDU for it has arrived within
     * {@code maxAgeMs}. Called periodically by the owning service so a
     * Metadata never followed by EOF cannot pin its reassembly buffer
     * forever.
     */
    public void expireInflight(long maxAgeMs) {
        expireInflight(maxAgeMs, System.currentTimeMillis());
    }

    synchronized void expireInflight(long maxAgeMs, long now) {
        if (inflight != null && now - inflightLastActivity > maxAgeMs) {
            LOG.warn("CFDP transaction {} stalled for over {} ms; failing",
                    inflight.transactionSeq, maxAgeMs);
            failInflight("transaction stalled: no PDU received within " + maxAgeMs + " ms");
        }
    }

    private void handleMetadata(byte[] bytes, CfdpPdu.Header header) {
        // Decode and validate the incoming Metadata *before* superseding the
        // in-flight transaction: a malformed or size-rejected Metadata must
        // not abort an unrelated healthy transfer.
        CfdpPdu.Metadata md = CfdpPdu.decodeMetadata(bytes, header);
        LOG.info("CFDP downlink Metadata: tx={} size={} src={} dst={}",
                header.transactionSeq, md.fileSize,
                ObjectNames.forLog(md.sourceFileName), ObjectNames.forLog(md.destinationFileName));
        if (md.fileSize < 0 || md.fileSize > maxFileSize) {
            // Drop without consuming any pending startDownload() transfer:
            // a single spoofed Metadata on the TM stream must not be able
            // to fail an operator's pending download. A legitimate oversize
            // downlink is caught by the pending-download timeout sweeper.
            LOG.error("Metadata declares file size {} outside [0, {}]; dropping",
                    md.fileSize, maxFileSize);
            return;
        }

        if (inflight != null) {
            if (inflight.transactionSeq == header.transactionSeq
                    && inflight.declaredSize == md.fileSize
                    && inflight.destinationFileName.equals(md.destinationFileName)) {
                // Retransmitted Metadata for the transaction already being
                // reassembled: ignore the duplicate rather than restarting.
                // It still counts as link activity for the stall sweeper.
                // A same-seq Metadata with a different size or destination is
                // a genuinely new (wrapped-seq) transaction and supersedes.
                inflightLastActivity = System.currentTimeMillis();
                LOG.warn("Ignoring duplicate Metadata for in-flight transaction {}",
                        inflight.transactionSeq);
                return;
            }
            LOG.warn("Got Metadata while transaction {} in progress; dropping previous",
                    inflight.transactionSeq);
            failInflight("superseded by new Metadata");
        }
        FprimeFileTransfer transfer = transferResolver.resolve(
                md.sourceFileName, md.destinationFileName, md.fileSize);
        transfer.setState(TransferState.RUNNING);
        inflight = new Reassembly(header.transactionSeq, md.destinationFileName,
                md.fileSize, transfer);
        inflightLastActivity = System.currentTimeMillis();
        listener.stateChanged(transfer);
        listener.verifierAck(transfer, AckStatus.PENDING,
                String.format("receiving %d bytes from %s", md.fileSize, md.sourceFileName));
    }

    private void handleFileData(byte[] bytes, CfdpPdu.Header header) {
        if (inflight == null) {
            LOG.warn("Got File Data with no in-flight transaction; dropping");
            return;
        }
        if (header.transactionSeq != inflight.transactionSeq) {
            LOG.warn("Got File Data for tx {} while reassembling tx {}; dropping",
                    header.transactionSeq, inflight.transactionSeq);
            return;
        }
        CfdpPdu.FileData data = CfdpPdu.decodeFileData(bytes, header);
        // Long arithmetic: offset is wire-controlled and int addition could
        // wrap negative and slip past the comparison.
        if (data.offset < 0 || (long) data.offset + data.dataSize > inflight.declaredSize) {
            LOG.error("File Data would overflow file: offset={} dataSize={} declared={}",
                    data.offset, data.dataSize, inflight.declaredSize);
            failInflight("overflow in File Data PDU");
            return;
        }
        System.arraycopy(bytes, data.dataStart, inflight.buffer, data.offset, data.dataSize);
        inflightLastActivity = System.currentTimeMillis();
        inflight.addExtent(data.offset, data.offset + data.dataSize);

        inflight.transfer.setTransferredSize(inflight.bytesReceived);
        listener.stateChanged(inflight.transfer);
    }

    private void handleEof(byte[] bytes, CfdpPdu.Header header) {
        if (inflight == null) {
            LOG.warn("Got EOF with no in-flight transaction");
            return;
        }
        if (header.transactionSeq != inflight.transactionSeq) {
            LOG.warn("Got EOF for tx {} while reassembling tx {}; dropping",
                    header.transactionSeq, inflight.transactionSeq);
            return;
        }
        CfdpPdu.Eof eof = CfdpPdu.decodeEof(bytes, header);
        if (eof.conditionCode != CfdpPdu.CONDITION_NO_ERROR) {
            LOG.warn("CFDP transaction {} ended with condition code {}",
                    header.transactionSeq, eof.conditionCode);
            failInflight("transaction cancelled: condition code " + eof.conditionCode);
            return;
        }
        if (eof.fileSize != inflight.declaredSize) {
            failInflight(String.format("EOF file size %d != Metadata file size %d",
                    eof.fileSize, inflight.declaredSize));
            return;
        }
        if (inflight.bytesReceived != inflight.declaredSize) {
            failInflight(String.format(
                    "incomplete file: covered %d of %d declared bytes",
                    inflight.bytesReceived, inflight.declaredSize));
            return;
        }
        int computed = CfdpChecksum.of(inflight.buffer);
        if (computed != eof.checksum) {
            failInflight(String.format("checksum mismatch: expected 0x%08x got 0x%08x",
                    eof.checksum, computed));
            return;
        }

        String objectName;
        try {
            objectName = ObjectNames.sanitize(inflight.destinationFileName);
        } catch (IllegalArgumentException e) {
            LOG.error("Rejecting unsafe destination file name '{}': {}",
                    ObjectNames.forLog(inflight.destinationFileName), e.getMessage());
            failInflight("unsafe destination path: " + e.getMessage());
            return;
        }
        // Hand storage off so blocking bucket/mirror I/O never stalls the TM
        // stream subscriber thread. Clear the in-flight slot first: the
        // reassembly is complete and the next Metadata may arrive immediately.
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
            LOG.info("CFDP downlink COMPLETE: {} ({} bytes) -> bucket {}",
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
