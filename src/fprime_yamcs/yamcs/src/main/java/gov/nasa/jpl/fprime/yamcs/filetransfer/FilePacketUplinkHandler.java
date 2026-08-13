package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.protobuf.TransferState;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpChecksum;
import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

/**
 * Uplinks a file as an {@code Fw::FilePacket} START / DATA×N / END sequence,
 * wrapping each packet in a CCSDS space packet on the file APID and handing
 * it to an {@link UplinkTransport}.
 */
public class FilePacketUplinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilePacketUplinkHandler.class);

    /**
     * Largest chunk that keeps the DATA packet within the CCSDS 16-bit
     * length field: descriptor (2) + header (5) + byteOffset (4) +
     * dataSize (2) + chunk must not exceed {@link SpacePacket#MAX_PAYLOAD_LEN}.
     */
    public static final int MAX_CHUNK_SIZE = SpacePacket.MAX_PAYLOAD_LEN
            - FilePacket.DESCRIPTOR_LEN - FilePacket.HEADER_LEN - 4 - 2;

    private final UplinkTransport transport;
    private final int fileApid;
    private final int chunkSize;
    private final TransferEventListener listener;
    // CCSDS sequence count for uplinked packets (14-bit, wraps). A command
    // postprocessor (e.g. FprimeCommandPostprocessor with CcsdsSeqCountFiller)
    // may re-patch this in place on links that have one configured; setting a
    // real count here keeps raw links without a postprocessor coherent too.
    // Atomic so the handler is safe regardless of which executor runs it.
    private final AtomicInteger seqCount = new AtomicInteger();

    public FilePacketUplinkHandler(UplinkTransport transport, int fileApid, int chunkSize,
                                   TransferEventListener listener) {
        if (chunkSize <= 0 || chunkSize > MAX_CHUNK_SIZE) {
            throw new IllegalArgumentException(
                    "chunkSize " + chunkSize + " outside [1, " + MAX_CHUNK_SIZE + "]");
        }
        this.transport = transport;
        this.fileApid = fileApid;
        this.chunkSize = chunkSize;
        this.listener = listener;
    }

    /**
     * Run the uplink to completion, updating the transfer record as it
     * progresses. Intended to run on a dedicated executor.
     */
    public void run(FprimeFileTransfer transfer, byte[] content) {
        try {
            transfer.setStartTime(System.currentTimeMillis());
            transfer.setState(TransferState.RUNNING);
            listener.stateChanged(transfer);
            LOG.info("Uplink START: id={} bucket={} object={} -> {} ({} bytes)",
                    transfer.getId(), transfer.getBucketName(), transfer.getObjectName(),
                    transfer.getRemotePath(), content.length);

            int seq = 0;
            send(FilePacket.encodeStart(seq, content.length,
                    transfer.getObjectName(), transfer.getRemotePath()));

            // DATA×N — update transferredSize after each chunk so the UI
            // progress bar animates.
            for (int offset = 0; offset < content.length; offset += chunkSize) {
                int len = Math.min(chunkSize, content.length - offset);
                seq++;
                send(FilePacket.encodeData(seq, offset, content, offset, len));
                transfer.setTransferredSize(offset + len);
                listener.stateChanged(transfer);
            }

            seq++;
            send(FilePacket.encodeEnd(seq, CfdpChecksum.of(content)));

            transfer.setTransferredSize(content.length);
            // Refused when the transfer was already failed (e.g. by a
            // shutdown sweep); do not announce a stale completion.
            if (transfer.setState(TransferState.COMPLETED)) {
                LOG.info("Uplink COMPLETE: id={} object={} ({} bytes)",
                        transfer.getId(), transfer.getObjectName(), content.length);
            }
        } catch (InterruptedException e) {
            // Service shutdown (executor.shutdownNow()); restore the flag so
            // the executor thread observes the interrupt.
            Thread.currentThread().interrupt();
            LOG.warn("Uplink INTERRUPTED: id={} object={}",
                    transfer.getId(), transfer.getObjectName());
            transfer.fail("interrupted");
        } catch (Exception e) {
            LOG.error("Uplink FAILED: id={} object={}",
                    transfer.getId(), transfer.getObjectName(), e);
            transfer.fail(e.getMessage() != null ? e.getMessage() : e.toString());
        } finally {
            listener.stateChanged(transfer);
        }
    }

    private void send(byte[] filePacket) throws Exception {
        int seq = seqCount.getAndUpdate(s -> (s + 1) & 0x3FFF);
        transport.send(SpacePacket.wrapTelecommand(filePacket, fileApid, seq));
    }
}
