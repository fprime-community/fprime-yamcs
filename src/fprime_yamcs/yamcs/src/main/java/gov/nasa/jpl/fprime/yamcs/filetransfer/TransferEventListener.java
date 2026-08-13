package gov.nasa.jpl.fprime.yamcs.filetransfer;

import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;

/**
 * Callbacks by which uplink/downlink handlers report transfer lifecycle
 * events back to the owning file transfer service.
 */
public interface TransferEventListener {

    /** The transfer's state or progress changed; notify transfer monitors. */
    void stateChanged(FprimeFileTransfer transfer);

    /**
     * Publish a verifier ack against the transfer's triggering command
     * (no-op if the transfer has none).
     */
    void verifierAck(FprimeFileTransfer transfer, AckStatus status, String message);
}
