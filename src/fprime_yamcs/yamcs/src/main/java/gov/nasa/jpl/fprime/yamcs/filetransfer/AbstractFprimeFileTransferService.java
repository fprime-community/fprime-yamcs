package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.Processor;
import org.yamcs.YamcsServer;
import org.yamcs.YamcsServerInstance;
import org.yamcs.buckets.Bucket;
import org.yamcs.buckets.BucketManager;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.commanding.CommandingManager;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.filetransfer.AbstractFileTransferService;
import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.filetransfer.FileTransferFilter;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.RemoteFileListMonitor;
import org.yamcs.filetransfer.TransferMonitor;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.protobuf.EntityInfo;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;
import org.yamcs.security.User;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.xtce.MetaCommand;

/**
 * Common scaffolding for F´ file transfer services: transfer bookkeeping,
 * monitor notification, verifier acks, remote file listings, and synthesis
 * of spacecraft commands (e.g. {@code FileDownlink.SendFile},
 * {@code FileManager.ListDirectory}) through a YAMCS processor.
 *
 * <p>Concrete services (Fw::FilePacket, CFDP, ...) supply the wire protocol:
 * how bytes get uplinked and how downlinked packets are reassembled.
 */
public abstract class AbstractFprimeFileTransferService extends AbstractFileTransferService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final String LOCAL_ENTITY_NAME = "ground";
    public static final String REMOTE_ENTITY_NAME = "spacecraft";

    // Custom verifier key reported back to YAMCS command history so
    // operators see the transfer outcome on the triggering command entry
    // in the command stack. Appears as Verifier_FileTransfer_Status etc.
    private static final String VERIFIER_KEY =
            CommandHistoryPublisher.Verifier_KEY_PREFIX + "FileTransfer";

    /**
     * Maximum retained transfer records. Terminal (completed/failed)
     * transfers beyond this are evicted oldest-first so a chattering link
     * cannot grow ground-server memory without bound.
     */
    protected static final int MAX_TRANSFER_HISTORY = 1000;

    /**
     * Maximum outstanding uplink transfers (the running task plus those
     * queued behind the single uplink worker). Each outstanding task pins
     * its full file contents in memory, so this mirrors the downlink
     * storage-backlog bound.
     */
    protected static final int MAX_PENDING_UPLOADS = 4;

    /**
     * Maximum concurrently pending download requests. Each pending record
     * is a non-terminal transfer exempt from history eviction; the timeout
     * sweeper reclaims stale ones, but the count is capped for symmetry
     * with {@link #MAX_PENDING_UPLOADS}.
     */
    protected static final int MAX_PENDING_DOWNLOADS = 64;

    /** Bound on how long an API thread may block on a bucket read. */
    protected static final int BUCKET_FETCH_TIMEOUT_S = 30;

    private final AtomicLong transferIdSeq = new AtomicLong(1);
    private final AtomicInteger pendingUplinks = new AtomicInteger();
    private final Map<Long, FprimeFileTransfer> transfers = new ConcurrentHashMap<>();
    private final List<TransferMonitor> transferMonitors = new CopyOnWriteArrayList<>();

    // Monitor callbacks run here rather than on the caller's thread: the
    // callers are TM stream subscribers holding the downlink handler
    // monitor, and a slow external monitor must not stall packet
    // processing. Single-threaded so notifications stay ordered.
    private final ExecutorService monitorNotifier = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "fprime-transfer-monitor-notifier");
        t.setDaemon(true);
        return t;
    });

    /**
     * Callback handed to protocol handlers so they can report transfer
     * progress without the service exposing mutation entry points on its
     * public API.
     */
    protected final TransferEventListener transferListener = new TransferEventListener() {
        @Override
        public void stateChanged(FprimeFileTransfer transfer) {
            notifyStateChanged(transfer);
        }

        @Override
        public void verifierAck(FprimeFileTransfer transfer, AckStatus status, String message) {
            publishVerifierAck(transfer, status, message);
        }
    };

    // Resolved by resolveProcessor() for spacecraft command synthesis.
    protected Processor processor;
    protected CommandingManager commandingManager;
    protected CommandHistoryPublisher commandHistoryPublisher;
    protected User systemUser;

    protected final RemoteFileListingHandler listingHandler =
            new RemoteFileListingHandler(REMOTE_ENTITY_NAME, monitorNotifier);

    // ------------------------------------------------------------------
    // Transfer bookkeeping
    // ------------------------------------------------------------------

    protected long nextTransferId() {
        return transferIdSeq.getAndIncrement();
    }

    protected void addTransfer(FprimeFileTransfer transfer) {
        transfers.put(transfer.getId(), transfer);
        evictOldTransfers();
    }

    private void evictOldTransfers() {
        // Snapshot the excess once: concurrent evictions could otherwise
        // shrink the map between reads and hand Stream.limit a negative.
        int excess = transfers.size() - MAX_TRANSFER_HISTORY;
        if (excess <= 0) {
            return;
        }
        transfers.values().stream()
                .filter(t -> t.getTransferState() == TransferState.COMPLETED
                        || t.getTransferState() == TransferState.FAILED)
                .sorted(Comparator.comparingLong(FprimeFileTransfer::getId))
                .limit(excess)
                .forEach(t -> transfers.remove(t.getId()));
    }

    @Override
    public List<FileTransfer> getTransfers(FileTransferFilter filter) {
        List<FileTransfer> all = new ArrayList<>(transfers.values());
        // Deterministic id order: ConcurrentHashMap iteration order is
        // arbitrary, and filter.limit must truncate to the newest (or oldest,
        // per filter.descending) transfers.
        Comparator<FileTransfer> byId = Comparator.comparingLong(FileTransfer::getId);
        if (filter == null) {
            all.sort(byId.reversed());
            return all;
        }
        all.sort(filter.descending ? byId.reversed() : byId);
        List<FileTransfer> result = new ArrayList<>();
        for (FileTransfer ft : all) {
            if (filter.start != TimeEncoding.INVALID_INSTANT
                    && ft.getCreationTime() < filter.start) {
                continue;
            }
            if (filter.stop != TimeEncoding.INVALID_INSTANT
                    && ft.getCreationTime() >= filter.stop) {
                continue;
            }
            if (filter.direction != null && ft.getDirection() != filter.direction) {
                continue;
            }
            if (filter.states != null && !filter.states.isEmpty()
                    && !filter.states.contains(ft.getTransferState())) {
                continue;
            }
            if (filter.localEntityId != null
                    && !filter.localEntityId.equals(ft.getLocalEntityId())) {
                continue;
            }
            if (filter.remoteEntityId != null
                    && !filter.remoteEntityId.equals(ft.getRemoteEntityId())) {
                continue;
            }
            result.add(ft);
        }
        if (filter.limit > 0 && result.size() > filter.limit) {
            result = result.subList(0, filter.limit);
        }
        return result;
    }

    @Override
    public FileTransfer getFileTransfer(long id) {
        return transfers.get(id);
    }

    @Override
    public void registerTransferMonitor(TransferMonitor monitor) {
        transferMonitors.add(monitor);
    }

    @Override
    public void unregisterTransferMonitor(TransferMonitor monitor) {
        transferMonitors.remove(monitor);
    }

    /** Push a transfer state change to all registered monitors. */
    protected void notifyStateChanged(FprimeFileTransfer transfer) {
        // Snapshot at submit time so the notification goes to the monitors
        // registered when the state change happened.
        List<TransferMonitor> recipients = new ArrayList<>(transferMonitors);
        try {
            monitorNotifier.execute(() -> {
                for (TransferMonitor m : recipients) {
                    try {
                        m.stateChanged(transfer);
                    } catch (Exception e) {
                        log.warn("Transfer monitor threw", e);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            log.debug("Dropping monitor notification after shutdown", e);
        }
    }

    /**
     * Publish a verifier ack to the YAMCS command history entry for the
     * command that triggered this transfer, so operators drilling into the
     * command stack see the transfer's progress alongside the standard acks.
     *
     * <p>No-op if the transfer wasn't triggered by a synthesized command.
     */
    protected void publishVerifierAck(FprimeFileTransfer transfer, AckStatus status,
                                      String message) {
        if (commandHistoryPublisher == null) {
            return;
        }
        CommandId cmdId = transfer.getTriggeringCommandId();
        if (cmdId == null) {
            return;
        }
        try {
            commandHistoryPublisher.publishAck(cmdId, VERIFIER_KEY,
                    System.currentTimeMillis(), status, message);
        } catch (Exception e) {
            log.debug("Failed to publish verifier ack for transfer {}", transfer.getId(), e);
        }
    }

    /** Flip a transfer to FAILED and notify monitors and command history. */
    protected void failTransfer(FprimeFileTransfer transfer, AckStatus ackStatus, String reason) {
        transfer.fail(reason);
        notifyStateChanged(transfer);
        publishVerifierAck(transfer, ackStatus, reason);
    }

    // ------------------------------------------------------------------
    // Pending download lifecycle (shared between protocols)
    // ------------------------------------------------------------------

    /**
     * startDownload() transfers waiting for the spacecraft to begin the
     * downlink, keyed by the destination file name the spacecraft echoes in
     * its Metadata PDU / Start packet.
     */
    protected final Map<String, FprimeFileTransfer> pendingDownloadsByPath =
            new ConcurrentHashMap<>();

    /** Hook for subclasses to decorate a freshly created download record. */
    protected void decorateDownloadTransfer(FprimeFileTransfer transfer) {
    }

    /**
     * Attach an API-level transfer record to a newly started downlink: a
     * pending startDownload() transfer keyed by the destination file name,
     * or a fresh record for an unsolicited (spacecraft initiated) downlink
     * so it still appears in the File Transfer UI.
     */
    protected FprimeFileTransfer resolveDownlinkTransfer(String bucketName, String transferType,
            String sourcePath, String destinationPath, int fileSize) {
        FprimeFileTransfer transfer = pendingDownloadsByPath.remove(destinationPath);
        if (transfer == null) {
            transfer = new FprimeFileTransfer(nextTransferId(), bucketName,
                    destinationPath, sourcePath, fileSize,
                    TransferDirection.DOWNLOAD, transferType, false);
            transfer.setStartTime(System.currentTimeMillis());
            decorateDownloadTransfer(transfer);
            addTransfer(transfer);
            log.info("Unsolicited {} downlink; created transfer record id={}",
                    transferType, transfer.getId());
        } else {
            // Update the totalSize now that the spacecraft has declared it.
            transfer.setTotalSize(fileSize);
        }
        return transfer;
    }

    /**
     * Validate a startDownload() request, register the pending transfer, and
     * dispatch the protocol's downlink command. The {@code noResponseHint}
     * names the spacecraft response being awaited (for ack/timeout text).
     */
    protected FprimeFileTransfer startDownloadCommon(MetaCommand command,
            String commandUnavailableMessage, String transferType,
            String sourcePath, Bucket destBucket, String destPath,
            String sourceFileNameArg, String destFileNameArg,
            Map<String, Object> extraCommandArgs,
            String noResponseHint) throws IOException {
        if (command == null) {
            throw new InvalidRequestException(commandUnavailableMessage);
        }
        if (sourcePath == null || sourcePath.isEmpty()) {
            throw new InvalidRequestException("sourcePath (file on spacecraft) is required");
        }
        if (destBucket == null) {
            throw new InvalidRequestException("destBucket is required");
        }
        if (destPath == null || destPath.isEmpty()) {
            // Default to the basename of the source path, so operators can
            // leave the destination blank in the UI.
            destPath = sourcePath.contains("/")
                    ? sourcePath.substring(sourcePath.lastIndexOf('/') + 1)
                    : sourcePath;
            if (destPath.isEmpty()) {
                throw new InvalidRequestException(
                        "cannot derive a destination file name from '" + sourcePath
                                + "'; specify destPath explicitly");
            }
        }

        long id = nextTransferId();
        FprimeFileTransfer transfer = new FprimeFileTransfer(
                id, destBucket.getName(), destPath, sourcePath, -1,
                TransferDirection.DOWNLOAD, transferType, false);
        transfer.setStartTime(System.currentTimeMillis());
        decorateDownloadTransfer(transfer);
        // Reject rather than overwrite: a displaced pending transfer could
        // never be resolved or timed out and would sit RUNNING forever.
        if (pendingDownloadsByPath.putIfAbsent(destPath, transfer) != null) {
            throw new InvalidRequestException(
                    "A download to '" + destPath + "' is already pending");
        }
        // Reserve-then-rollback: check the bound only after inserting so
        // concurrent callers cannot slip past MAX_PENDING_DOWNLOADS.
        if (pendingDownloadsByPath.size() > MAX_PENDING_DOWNLOADS) {
            pendingDownloadsByPath.remove(destPath, transfer);
            throw new InvalidRequestException("Too many pending downloads ("
                    + MAX_PENDING_DOWNLOADS + "); wait for one to complete or time out");
        }
        addTransfer(transfer);
        notifyStateChanged(transfer);

        try {
            // Fixed extras first (e.g. channelId/priority for F´ SendFile),
            // then the path arguments so they cannot be overridden.
            Map<String, Object> args = new HashMap<>(extraCommandArgs);
            args.put(sourceFileNameArg, sourcePath);
            args.put(destFileNameArg, destPath);
            CommandId cmdId = dispatchCommand(command, args,
                    getClass().getSimpleName(), (int) (id & 0x7FFFFFFF));
            // Remember the CommandId so verifier acks can be published
            // against this command's history entry as the transfer
            // progresses through RUNNING -> COMPLETED/FAILED.
            transfer.setTriggeringCommandId(cmdId);
            publishVerifierAck(transfer, AckStatus.SCHEDULED,
                    "waiting for spacecraft " + noResponseHint);
            log.info("{} downlink START: id={} source={} (on spacecraft) -> bucket {}/{}",
                    transferType, id, sourcePath, destBucket.getName(), destPath);
        } catch (Exception e) {
            log.error("Failed to dispatch downlink command for transfer {}", id, e);
            pendingDownloadsByPath.remove(destPath, transfer);
            failTransfer(transfer, AckStatus.NOK, "command dispatch failed: " + e.getMessage());
            throw new IOException("Failed to dispatch downlink command: " + e.getMessage(), e);
        }
        return transfer;
    }

    /**
     * Fail any pending download whose start time is older than
     * {@code downloadTimeoutMs} — the "spacecraft never responded" case.
     * Transfers already linked to an in-flight reassembly are not in the
     * pending map.
     */
    protected void sweepPendingDownloadTimeouts(long downloadTimeoutMs, String noResponseHint) {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, FprimeFileTransfer> entry :
                new ArrayList<>(pendingDownloadsByPath.entrySet())) {
            FprimeFileTransfer t = entry.getValue();
            long age = now - t.getStartTime();
            if (age < downloadTimeoutMs) {
                continue;
            }
            // Best-effort atomic remove: if the downlink handler beat us to
            // it, remove() returns false and the state change is skipped.
            if (!pendingDownloadsByPath.remove(entry.getKey(), t)) {
                continue;
            }
            log.warn("Download timeout: id={} remotePath={} after {} ms — no {} received",
                    t.getId(), t.getRemotePath(), age, noResponseHint);
            String reason = "timeout after " + age + " ms: no " + noResponseHint
                    + " for '" + t.getRemotePath() + "'";
            t.fail(reason);
            notifyStateChanged(t);
            publishVerifierAck(t, AckStatus.TIMEOUT, reason);
        }
    }

    /**
     * Queue an uplink task, bounding the backlog: each queued task pins the
     * file contents in memory, so past {@link #MAX_PENDING_UPLOADS} the
     * request is rejected instead of queued. Registers the transfer and
     * notifies monitors before submission.
     */
    protected void submitUplink(ExecutorService uplinkExecutor,
                                FprimeFileTransfer transfer, Runnable task) {
        reserveUplinkSlot();
        try {
            submitReservedUplink(uplinkExecutor, transfer, task);
        } catch (RuntimeException e) {
            releaseUplinkSlot();
            throw e;
        }
    }

    /**
     * Atomically reserve an uplink backlog slot: increment first, roll back
     * if over the bound, so concurrent submissions cannot slip past
     * {@link #MAX_PENDING_UPLOADS}. Reserve before fetching file contents
     * so rejected requests never pin the bytes in memory.
     */
    protected void reserveUplinkSlot() {
        if (pendingUplinks.incrementAndGet() > MAX_PENDING_UPLOADS) {
            pendingUplinks.decrementAndGet();
            throw new InvalidRequestException("uplink backlog: " + MAX_PENDING_UPLOADS
                    + " transfers already queued");
        }
    }

    /** Release a slot taken by {@link #reserveUplinkSlot()} without submitting. */
    protected void releaseUplinkSlot() {
        pendingUplinks.decrementAndGet();
    }

    /**
     * Submit an uplink whose backlog slot is already reserved. On failure
     * the transfer is failed and the exception rethrown; releasing the
     * reserved slot is the caller's responsibility (single owner).
     */
    protected void submitReservedUplink(ExecutorService uplinkExecutor,
                                        FprimeFileTransfer transfer, Runnable task) {
        addTransfer(transfer);
        notifyStateChanged(transfer);
        try {
            uplinkExecutor.submit(() -> {
                try {
                    task.run();
                } finally {
                    pendingUplinks.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            failTransfer(transfer, AckStatus.NOK, "uplink executor rejected: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Shared upload path: validates the request, reserves a backlog slot
     * before fetching so rejected requests never pin file bytes in memory,
     * builds the transfer (with {@link #decorateUploadTransfer} applied),
     * and submits it to the uplink executor.
     */
    protected FileTransfer startUploadCommon(Bucket sourceBucket, String objectName,
                                             String remotePath, String transferType,
                                             int maxFileSize, ExecutorService uplinkExecutor,
                                             UplinkRunner runner) throws IOException {
        // Parameter semantics per YAMCS FileTransferApi.createTransfer:
        // destinationEntity is the remote entity *name* ("spacecraft"); the
        // remotePath string is the actual path on the spacecraft where the
        // file should land.
        if (sourceBucket == null) {
            throw new InvalidRequestException("sourceBucket is required");
        }
        if (objectName == null || objectName.isEmpty()) {
            throw new InvalidRequestException("objectName is required");
        }
        reserveUplinkSlot();
        boolean submitted = false;
        try {
            byte[] content = fetchObject(sourceBucket, objectName);
            if (content == null) {
                throw new InvalidRequestException(
                        "No such object '" + objectName + "' in bucket " + sourceBucket.getName());
            }
            if (content.length > maxFileSize) {
                throw new InvalidRequestException("Object '" + objectName + "' is "
                        + content.length + " bytes, larger than maxFileSize " + maxFileSize);
            }

            String dest = (remotePath == null || remotePath.isEmpty()) ? objectName : remotePath;
            FprimeFileTransfer transfer = new FprimeFileTransfer(
                    nextTransferId(), sourceBucket.getName(), objectName, dest,
                    content.length, TransferDirection.UPLOAD, transferType, false);
            decorateUploadTransfer(transfer);
            submitReservedUplink(uplinkExecutor, transfer,
                    () -> runner.run(transfer, content));
            submitted = true;
            return transfer;
        } finally {
            if (!submitted) {
                releaseUplinkSlot();
            }
        }
    }

    /** Runs a submitted uplink task with the fetched file contents. */
    @FunctionalInterface
    protected interface UplinkRunner {
        void run(FprimeFileTransfer transfer, byte[] content);
    }

    /** Hook for subclasses to stamp protocol-specific upload transfer fields. */
    protected void decorateUploadTransfer(FprimeFileTransfer transfer) {
    }

    /**
     * Flip every non-terminal (queued/running/paused) transfer to FAILED.
     * Called from {@code doStop()} so stopped services never leave transfers
     * stranded in a non-terminal state that eviction cannot reclaim.
     */
    protected void failNonTerminalTransfers(String reason) {
        for (FprimeFileTransfer t : transfers.values()) {
            TransferState s = t.getTransferState();
            if (s != TransferState.COMPLETED && s != TransferState.FAILED) {
                failTransfer(t, AckStatus.NOK, reason);
            }
        }
    }

    /**
     * Stop the monitor-notification thread once queued notifications drain.
     * Called from {@code doStop()} after {@link #failNonTerminalTransfers}
     * so shutdown-failure notifications still reach monitors.
     */
    protected void shutdownMonitorNotifier() {
        monitorNotifier.shutdown();
    }

    // ------------------------------------------------------------------
    // Entities
    // ------------------------------------------------------------------

    /** API-level local entity id; services with configured ids override. */
    protected long localApiEntityId() {
        return FprimeFileTransfer.GROUND_ENTITY_ID;
    }

    /** API-level remote entity id; services with configured ids override. */
    protected long remoteApiEntityId() {
        return FprimeFileTransfer.SPACECRAFT_ENTITY_ID;
    }

    @Override
    public List<EntityInfo> getLocalEntities() {
        return List.of(EntityInfo.newBuilder()
                .setId(localApiEntityId())
                .setName(LOCAL_ENTITY_NAME)
                .build());
    }

    @Override
    public List<EntityInfo> getRemoteEntities() {
        return List.of(EntityInfo.newBuilder()
                .setId(remoteApiEntityId())
                .setName(REMOTE_ENTITY_NAME)
                .build());
    }

    // ------------------------------------------------------------------
    // Spacecraft command synthesis
    // ------------------------------------------------------------------

    /**
     * Resolve the first processor of this instance for command synthesis.
     * Returns false (and logs) if no processor is available.
     */
    protected boolean resolveProcessor() {
        YamcsServerInstance ysi = YamcsServer.getServer().getInstance(yamcsInstance);
        this.processor = ysi == null ? null : ysi.getFirstProcessor();
        if (processor == null) {
            log.warn("No processor available; spacecraft command synthesis disabled");
            return false;
        }
        this.commandingManager = processor.getCommandingManager();
        this.commandHistoryPublisher = processor.getCommandHistoryPublisher();
        this.systemUser = YamcsServer.getServer().getSecurityStore().getSystemUser();
        return true;
    }

    /**
     * Find a MetaCommand by qualified name, or — when {@code configuredName}
     * is empty — by a qualified-name suffix (e.g. {@code "SendFile"}),
     * enabling auto-discovery against generated F´ dictionaries.
     */
    protected MetaCommand findCommand(String configuredName, String suffix) {
        if (processor == null) {
            return null;
        }
        String name = configuredName;
        if ((name == null || name.isEmpty()) && suffix != null && !suffix.isEmpty()) {
            List<String> names = new ArrayList<>();
            for (MetaCommand cmd : processor.getMdb().getMetaCommands()) {
                names.add(cmd.getQualifiedName());
            }
            List<String> candidates = suffixCandidates(names, suffix);
            if (candidates.size() > 1) {
                // Refuse ambiguous auto-discovery: which command wins would
                // depend on MDB iteration order, and the chosen command is
                // dispatched to the spacecraft. Require explicit config.
                log.warn("Multiple commands match suffix '{}': {}; refusing auto-discovery — "
                        + "configure the qualified command name explicitly", suffix, candidates);
                return null;
            }
            name = candidates.isEmpty() ? null : candidates.get(0);
            log.info("Auto-discovered command for suffix '{}': {}", suffix, name);
        }
        return name == null || name.isEmpty() ? null : processor.getMdb().getMetaCommand(name);
    }

    /**
     * Qualified names whose final segment equals {@code suffix}. Matching
     * is on a name-segment boundary so e.g. {@code AbortSendFile} is never
     * mistaken for {@code SendFile}.
     */
    static List<String> suffixCandidates(Collection<String> qualifiedNames, String suffix) {
        List<String> candidates = new ArrayList<>();
        for (String qn : qualifiedNames) {
            if (qn.endsWith("/" + suffix) || qn.endsWith("." + suffix)) {
                candidates.add(qn);
            }
        }
        return candidates;
    }

    /**
     * Fetch a bucket object with a bounded wait, translating async failures
     * into the API-facing exception types.
     */
    protected static byte[] fetchObject(Bucket bucket, String objectName) throws IOException {
        CompletableFuture<byte[]> future = bucket.getObjectAsync(objectName);
        try {
            return future.get(BUCKET_FETCH_TIMEOUT_S, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading '" + objectName
                    + "' from bucket " + bucket.getName(), e);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new IOException("Timed out reading '" + objectName
                    + "' from bucket " + bucket.getName(), e);
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IOException("Failed to read '" + objectName + "' from bucket "
                    + bucket.getName() + ": " + cause.getMessage(), cause);
        }
    }

    /**
     * Build and dispatch a spacecraft command, returning its CommandId.
     *
     * <p>Commands are dispatched as the YAMCS system user (matching the
     * built-in {@code org.yamcs.cfdp.CfdpService} pattern): a user granted
     * file-transfer privileges implicitly gains the authority to send the
     * configured transfer commands, bypassing per-user command authorization.
     * Deployments should gate file-transfer privileges accordingly.
     */
    protected CommandId dispatchCommand(MetaCommand command, Map<String, Object> args,
                                        String origin, int sequenceNumber) throws Exception {
        PreparedCommand pc = commandingManager.buildCommand(
                command, args, origin, sequenceNumber, systemUser);
        commandingManager.sendCommand(systemUser, pc);
        return pc.getCommandId();
    }

    protected Bucket getOrCreateBucket(String name) throws Exception {
        BucketManager bm = YamcsServer.getServer().getBucketManager();
        Bucket b = bm.getBucket(name);
        if (b == null) {
            log.info("Bucket {} not found, creating", name);
            b = bm.createBucket(name);
        }
        return b;
    }

    // ------------------------------------------------------------------
    // Remote file listing — delegated to RemoteFileListingHandler
    // ------------------------------------------------------------------

    @Override
    public ListFilesResponse getFileList(String localEntity, String remoteEntity,
                                         String remotePath, Map<String, Object> options) {
        return listingHandler.getFileList(normalizeDirName(remotePath));
    }

    @Override
    public void saveFileList(ListFilesResponse listing) {
        // Normalize the cache key the same way getFileList does, so a
        // listing saved for "" is found again when queried as ".".
        if (listing != null
                && !listing.getRemotePath().equals(normalizeDirName(listing.getRemotePath()))) {
            listing = listing.toBuilder()
                    .setRemotePath(normalizeDirName(listing.getRemotePath()))
                    .build();
        }
        listingHandler.saveFileList(listing);
    }

    @Override
    public void registerRemoteFileListMonitor(RemoteFileListMonitor monitor) {
        listingHandler.registerMonitor(monitor);
    }

    @Override
    public void unregisterRemoteFileListMonitor(RemoteFileListMonitor monitor) {
        listingHandler.unregisterMonitor(monitor);
    }

    @Override
    public void notifyRemoteFileListMonitors(ListFilesResponse listing) {
        listingHandler.notifyMonitors(listing);
    }

    @Override
    public Set<RemoteFileListMonitor> getRemoteFileListMonitors() {
        return listingHandler.getMonitors();
    }

    protected static String normalizeDirName(String remotePath) {
        return (remotePath == null || remotePath.isEmpty()) ? "." : remotePath;
    }
}
