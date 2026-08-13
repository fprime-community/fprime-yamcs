package gov.nasa.jpl.fprime.yamcs.filetransfer;

import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;

/**
 * In-memory {@link FileTransfer} record shared by the F´ file transfer
 * services (Fw::FilePacket, CFDP, ...).
 */
public class FprimeFileTransfer implements FileTransfer {

    /**
     * Default entity ids for the FileTransferService interface, used when
     * the owning service does not configure protocol-level entity ids.
     * YAMCS only requires them to be unique within the respective
     * local/remote sets.
     */
    public static final long GROUND_ENTITY_ID = 1L;
    public static final long SPACECRAFT_ENTITY_ID = 2L;

    private final long id;
    private final String bucketName;
    private final String objectName;
    private final String remotePath;
    private final String transferType;
    private final boolean reliable;
    private volatile long totalSize;
    private final TransferDirection direction;
    private final long creationTime = System.currentTimeMillis();

    private volatile long startTime;
    private volatile long transferredSize;
    // QUEUED until the protocol handler actually starts moving bytes
    // (uplink: dequeued by the uplink executor; downlink: first packet
    // received from the spacecraft).
    private volatile TransferState state = TransferState.QUEUED;
    private volatile String failureReason;
    // For downlink transfers triggered via startDownload(), this is the
    // CommandId of the synthesized spacecraft command. Verification results
    // are published against this CommandId so operators see the transfer
    // outcome in the command stack. Null for uplinks (no triggering command)
    // and for unsolicited downlinks (command stack issued directly).
    private volatile CommandId triggeringCommandId;
    private volatile long localEntityId = GROUND_ENTITY_ID;
    private volatile long remoteEntityId = SPACECRAFT_ENTITY_ID;

    public FprimeFileTransfer(long id, String bucketName, String objectName,
                              String remotePath, long totalSize, TransferDirection direction,
                              String transferType, boolean reliable) {
        this.id = id;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.remotePath = remotePath;
        this.totalSize = totalSize;
        this.direction = direction;
        this.transferType = transferType;
        this.reliable = reliable;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public String getRemotePath() {
        return remotePath;
    }

    @Override
    public Long getLocalEntityId() {
        return localEntityId;
    }

    @Override
    public Long getRemoteEntityId() {
        return remoteEntityId;
    }

    /** Report the owning service's configured entity ids through the API. */
    public void setEntityIds(long localEntityId, long remoteEntityId) {
        this.localEntityId = localEntityId;
        this.remoteEntityId = remoteEntityId;
    }

    @Override
    public TransferDirection getDirection() {
        return direction;
    }

    @Override
    public long getTotalSize() {
        return totalSize;
    }

    @Override
    public long getTransferredSize() {
        return transferredSize;
    }

    @Override
    public TransferState getTransferState() {
        return state;
    }

    @Override
    public boolean isReliable() {
        return reliable;
    }

    @Override
    public String getFailuredReason() {
        return failureReason;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public String getTransferType() {
        return transferType;
    }

    @Override
    public boolean pausable() {
        return false;
    }

    @Override
    public boolean cancellable() {
        return false;
    }

    public void setStartTime(long t) {
        this.startTime = t;
    }

    public void setTransferredSize(long n) {
        this.transferredSize = n;
    }

    public void setTotalSize(long n) {
        this.totalSize = n;
    }

    /**
     * Transition the transfer state. COMPLETED and FAILED are terminal:
     * once reached, transitions to a different state are refused (returns
     * false) so a late storage-worker completion cannot resurrect a
     * transfer already failed by shutdown, and vice versa.
     */
    public synchronized boolean setState(TransferState s) {
        if ((state == TransferState.COMPLETED || state == TransferState.FAILED)
                && state != s) {
            return false;
        }
        this.state = s;
        return true;
    }

    public void setFailureReason(String r) {
        this.failureReason = r;
    }

    /**
     * Atomically fail the transfer: the reason is recorded only if the
     * FAILED transition is accepted, so a failure sweep racing a completing
     * worker cannot leave a COMPLETED transfer with a stale failure reason.
     *
     * @return true if the transfer transitioned to FAILED
     */
    public synchronized boolean fail(String reason) {
        if (!setState(TransferState.FAILED)) {
            return false;
        }
        this.failureReason = reason;
        return true;
    }

    public void setTriggeringCommandId(CommandId id) {
        this.triggeringCommandId = id;
    }

    public CommandId getTriggeringCommandId() {
        return triggeringCommandId;
    }
}
