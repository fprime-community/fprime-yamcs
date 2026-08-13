package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.protobuf.TransferState;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpChecksum;
import gov.nasa.jpl.fprime.yamcs.packet.CfdpPdu;
import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

/**
 * Uplinks a file as a class-1 CFDP transaction (Metadata / File Data×N /
 * EOF), framing each PDU behind the F´ {@code FW_PACKET_FILE} descriptor
 * inside a CCSDS space packet on the CFDP APID and handing it to an
 * {@link UplinkTransport}. The descriptor is how F´'s CfdpManager
 * recognizes file traffic on the uplink.
 */
public class CfdpUplinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CfdpUplinkHandler.class);

    /**
     * Largest chunk that keeps the File Data PDU within the CFDP 16-bit data
     * field length and the resulting space packet within the CCSDS 16-bit
     * length field: descriptor (2) + PDU header (8) + offset (4) + chunk.
     */
    public static final int MAX_CHUNK_SIZE = Math.min(0xFFFF - 4,
            SpacePacket.MAX_PAYLOAD_LEN - FilePacket.DESCRIPTOR_LEN
                    - CfdpPdu.HEADER_LEN - 4);

    private final UplinkTransport transport;
    private final int cfdpApid;
    private final int chunkSize;
    private final int localEntityId;
    private final int remoteEntityId;
    private final TransferEventListener listener;
    // CFDP transaction sequence number (16-bit, wraps) and CCSDS sequence
    // count (14-bit, wraps; may be re-patched by a link postprocessor).
    // Atomic so the handler is safe regardless of which executor runs it.
    // Seeded from wall-clock seconds so a service restart does not reuse
    // recent transaction numbers against a receiver correlating on
    // (source entity, transaction seq).
    private final AtomicInteger transactionSeq =
            new AtomicInteger((int) ((System.currentTimeMillis() / 1000) & 0xFFFF));
    private final AtomicInteger seqCount = new AtomicInteger();

    public CfdpUplinkHandler(UplinkTransport transport, int cfdpApid, int chunkSize,
                             int localEntityId, int remoteEntityId,
                             TransferEventListener listener) {
        if (chunkSize <= 0 || chunkSize > MAX_CHUNK_SIZE) {
            throw new IllegalArgumentException(
                    "chunkSize " + chunkSize + " outside [1, " + MAX_CHUNK_SIZE + "]");
        }
        this.transport = transport;
        this.cfdpApid = cfdpApid;
        this.chunkSize = chunkSize;
        this.localEntityId = localEntityId;
        this.remoteEntityId = remoteEntityId;
        this.listener = listener;
    }

    /**
     * Run the uplink transaction to completion, updating the transfer record
     * as it progresses. Intended to run on a dedicated executor.
     */
    public void run(FprimeFileTransfer transfer, byte[] content) {
        try {
            transfer.setStartTime(System.currentTimeMillis());
            transfer.setState(TransferState.RUNNING);
            listener.stateChanged(transfer);
            int txSeq = nextTransactionSeq();
            LOG.info("CFDP uplink START: id={} tx={} object={} -> {} ({} bytes)",
                    transfer.getId(), txSeq, transfer.getObjectName(),
                    transfer.getRemotePath(), content.length);

            send(CfdpPdu.encodeMetadata(localEntityId, remoteEntityId, txSeq,
                    content.length, transfer.getObjectName(), transfer.getRemotePath()));

            for (int offset = 0; offset < content.length; offset += chunkSize) {
                int len = Math.min(chunkSize, content.length - offset);
                send(CfdpPdu.encodeFileData(localEntityId, remoteEntityId, txSeq,
                        offset, content, offset, len));
                transfer.setTransferredSize(offset + len);
                listener.stateChanged(transfer);
            }

            send(CfdpPdu.encodeEof(localEntityId, remoteEntityId, txSeq,
                    CfdpPdu.CONDITION_NO_ERROR, CfdpChecksum.of(content), content.length));

            transfer.setTransferredSize(content.length);
            // Refused when the transfer was already failed (e.g. by a
            // shutdown sweep); do not announce a stale completion.
            if (transfer.setState(TransferState.COMPLETED)) {
                LOG.info("CFDP uplink COMPLETE: id={} tx={} ({} bytes)",
                        transfer.getId(), txSeq, content.length);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("CFDP uplink INTERRUPTED: id={} object={}",
                    transfer.getId(), transfer.getObjectName());
            transfer.fail("interrupted");
        } catch (Exception e) {
            LOG.error("CFDP uplink FAILED: id={} object={}",
                    transfer.getId(), transfer.getObjectName(), e);
            transfer.fail(e.getMessage() != null ? e.getMessage() : e.toString());
        } finally {
            listener.stateChanged(transfer);
        }
    }

    private int nextTransactionSeq() {
        return transactionSeq.getAndUpdate(s -> (s + 1) & 0xFFFF);
    }

    private void send(byte[] pdu) throws Exception {
        // F´ routes uplinked packets by the leading FwPacketDescriptorType
        // word; CfdpManager only accepts PDUs behind FW_PACKET_FILE.
        byte[] framed = new byte[FilePacket.DESCRIPTOR_LEN + pdu.length];
        framed[0] = (byte) (FilePacket.FILE_DESCRIPTOR >> 8);
        framed[1] = (byte) FilePacket.FILE_DESCRIPTOR;
        System.arraycopy(pdu, 0, framed, FilePacket.DESCRIPTOR_LEN, pdu.length);
        int seq = seqCount.getAndUpdate(s -> (s + 1) & 0x3FFF);
        transport.send(SpacePacket.wrapTelecommand(framed, cfdpApid, seq));
    }
}
