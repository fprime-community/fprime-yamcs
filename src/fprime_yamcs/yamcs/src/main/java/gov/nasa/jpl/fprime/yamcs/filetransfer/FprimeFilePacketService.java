package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.buckets.Bucket;
import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.TransferOptions;
import org.yamcs.protobuf.FileTransferCapabilities;
import org.yamcs.xtce.MetaCommand;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

/**
 * Handles {@code Fw::FilePacket} file transfer to and from F´ over the
 * {@code FW_PACKET_FILE} (APID 3) channel, integrating with YAMCS's native
 * {@link org.yamcs.filetransfer.FileTransferService} interface.
 *
 * <p>Because this service implements {@code FileTransferService}, it is
 * auto-discovered by YAMCS's built-in {@code FileTransferApi} REST endpoints
 * (see {@code /api/filetransfer/{instance}/services}) and appears in the
 * stock {@code yamcs-web} File Transfer UI alongside any other configured
 * file transfer providers (e.g. CFDP).
 *
 * <p><b>Downlink</b>: subscribes to a TM stream (default {@code tm_realtime}),
 * filters for the file APID, and delegates reassembly and storage to
 * {@link FilePacketDownlinkHandler}. Downlinks are triggered either by
 * {@link #startDownload} (which synthesizes an F´ {@code FileDownlink.SendFile}
 * command) or unsolicited by the spacecraft.
 *
 * <p><b>Uplink</b>: {@link #startUpload} reads the specified bucket object and
 * hands it to {@link FilePacketUplinkHandler}, which generates the
 * {@code Fw::FilePacket} sequence and emits each packet through the configured
 * {@link UplinkTransport}. Any YAMCS TC data link works — a CCSDS TC frame
 * virtual channel (TM/TC pipeline) or a raw space packet link.
 *
 * <p><b>Remote file listing</b>: {@link #fetchFileList} synthesizes an F´
 * {@code FileManager.ListDirectory} command and {@link RemoteFileListingHandler}
 * reassembles the directory contents from the resulting F´ events. This
 * requires the {@code fprime-yamcs-events} publisher to be running (it
 * republishes F´ events onto the {@code events_realtime} stream this service
 * subscribes to); without it, listings time out and are reported failed.
 *
 * <p>Protocol limitations (inherent to {@code Fw::FilePacket}):
 * <ul>
 *   <li>One in-flight downlink transfer at a time
 *   <li>Uplinks run serially on a single executor thread
 *   <li>No retransmit on either side (fire-and-forget)
 *   <li>No pause / resume / cancel
 * </ul>
 *
 * <p>Configured under {@code services:} in the instance configuration:
 * <pre>
 *   - class: gov.nasa.jpl.fprime.yamcs.filetransfer.FprimeFilePacketService
 *     args:
 *       inStream: tm_realtime          # default
 *       bucket: fprimeFilesIn          # incoming bucket
 *       fileApid: 3                    # default; FW_PACKET_FILE
 *       uplinkLink: UDP_TC_OUT.vc1     # YAMCS TC link to route through
 *       uplinkChunkSize: 128           # bytes per Fw::FilePacket DataPacket
 *       interPacketDelayMs: 20         # pacing delay between uplink packets
 *       downlinkMirrorDir: ""          # optional local mirror (default off; set a
 *                                      # service-owned directory to enable — avoid
 *                                      # world-writable locations like /tmp)
 *       maxFileSize: 268435456         # per-file cap: downlink allocation and
 *                                      # uplink object size, in bytes
 *       downloadTimeoutMs: 30000       # max wait for the F´ Start packet
 *       fileDownlinkCommand: ""        # qualified MDB name; auto-discovered
 *                                      # by "SendFile" suffix when empty
 *       sourceFileNameArg: sourceFileName  # SendFile source-path argument name
 *       destFileNameArg: destFileName      # SendFile destination-path argument name
 *       listDirectoryCommand: ""       # qualified MDB name; auto-discovered
 *                                      # by "ListDirectory" suffix when empty
 *       listDirDirNameArg: dirName     # ListDirectory directory argument name
 *       eventsStream: events_realtime  # stream carrying F´ events used to
 *                                      # collect remote file-listing results
 * </pre>
 */
public class FprimeFilePacketService extends AbstractFprimeFileTransferService
        implements StreamSubscriber {

    // 256 MiB: generously above any realistic Fw::FilePacket downlink while
    // bounding what a corrupt/malicious START packet can allocate.
    private static final int DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;

    // F´ ComCfg::Apid::FW_PACKET_FILE — see default/config/ComCfg.fpp in nasa/fprime.
    private static final int DEFAULT_FILE_APID = 3;

    private static final String TRANSFER_TYPE = "FwFilePacket";

    // A reassembly with no packet activity for this long is failed by the
    // sweeper, releasing its buffer.
    private static final long INFLIGHT_STALL_TIMEOUT_MS = 60_000;

    // Configuration
    private String inStreamName;
    private String bucketName;
    private int fileApid;
    private int maxFileSize;
    private Path downlinkMirrorDir;
    private String uplinkLinkName;
    private int uplinkChunkSize;
    private long interPacketDelayMs;
    private String fileDownlinkCommandName;
    private String sourceFileNameArg;
    private String destFileNameArg;
    private Map<String, Object> downlinkCommandArgs;
    private String listDirectoryCommandName;
    private String listDirDirNameArg;
    // Max wall-clock time a startDownload() transfer may wait for a Start
    // packet from F´. If the spacecraft never emits one (command rejected,
    // file missing, link dropped, ...) the transfer is flipped to FAILED
    // instead of hanging forever in RUNNING state.
    private long downloadTimeoutMs;

    // Runtime
    private Stream inStream;
    private Stream eventsStream;
    private String eventsStreamName;
    private RemoteFileListingHandler eventsSubscriber;
    private FilePacketDownlinkHandler downlinkHandler;
    private FilePacketUplinkHandler uplinkHandler;
    private ExecutorService uplinkExecutor;
    // Storage worker: completed downlink files are written to the bucket and
    // mirror off the TM stream subscriber thread.
    private ExecutorService storageExecutor;
    // Periodic sweeper that flips stuck pending-download transfers to
    // FAILED. Runs on a separate single-thread scheduler so a slow uplink
    // can't block timeout enforcement.
    private ScheduledExecutorService timeoutScheduler;
    MetaCommand fileDownlinkCommand;   // may be null if not in MDB
    MetaCommand listDirectoryCommand;  // may be null if not in MDB

    // ------------------------------------------------------------------
    // Spec / configuration
    // ------------------------------------------------------------------

    @Override
    public Spec getSpec() {
        Spec spec = new Spec();
        spec.addOption("inStream", OptionType.STRING).withDefault("tm_realtime");
        spec.addOption("bucket", OptionType.STRING).withDefault("fprimeFilesIn");
        spec.addOption("fileApid", OptionType.INTEGER).withDefault(DEFAULT_FILE_APID);
        spec.addOption("maxFileSize", OptionType.INTEGER).withDefault(DEFAULT_MAX_FILE_SIZE);
        // Mirroring defaults off: a world-writable default like /tmp could be
        // pre-created as a symlink by a local attacker before service start.
        // Point this at a directory owned by the YAMCS user to enable.
        spec.addOption("downlinkMirrorDir", OptionType.STRING).withDefault("");
        // Route uplink through the YAMCS-configured TC data link. The
        // service accepts any TcDataLink and fails to start otherwise.
        spec.addOption("uplinkLink", OptionType.STRING).withDefault("UDP_TC_OUT.vc1");
        spec.addOption("uplinkChunkSize", OptionType.INTEGER).withDefault(128);
        spec.addOption("interPacketDelayMs", OptionType.INTEGER).withDefault(20);
        // Downlink: qualified name of the F´ command that triggers a
        // FileDownlink on the spacecraft, plus the names of its source and
        // destination path arguments. Auto-discovered by suffix when empty.
        spec.addOption("fileDownlinkCommand", OptionType.STRING).withDefault("");
        spec.addOption("sourceFileNameArg", OptionType.STRING).withDefault("sourceFileName");
        spec.addOption("destFileNameArg", OptionType.STRING).withDefault("destFileName");
        // Fixed values for any remaining downlink-command arguments beyond
        // the two path arguments.
        spec.addOption("downlinkCommandArgs", OptionType.ANY);
        spec.addOption("listDirectoryCommand", OptionType.STRING).withDefault("");
        spec.addOption("listDirDirNameArg", OptionType.STRING).withDefault("dirName");
        // Stream carrying the decoded F´ events republished by the
        // fprime-yamcs-events process; consumed for directory listings.
        spec.addOption("eventsStream", OptionType.STRING).withDefault("events_realtime");
        // How long to wait for F´ to emit a Start packet after a
        // FileDownlink command is synthesized before flipping the pending
        // transfer to FAILED. 30 seconds is generous for a small fleet;
        // increase for links with high RTT.
        spec.addOption("downloadTimeoutMs", OptionType.INTEGER).withDefault(30000);
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config)
            throws InitException {
        super.init(yamcsInstance, serviceName, config);
        this.inStreamName = config.getString("inStream", "tm_realtime");
        this.bucketName = config.getString("bucket", "fprimeFilesIn");
        this.fileApid = config.getInt("fileApid", DEFAULT_FILE_APID);
        this.maxFileSize = config.getInt("maxFileSize", DEFAULT_MAX_FILE_SIZE);
        String mirror = config.getString("downlinkMirrorDir", "");
        this.downlinkMirrorDir = mirror.isEmpty() ? null : Paths.get(mirror);
        this.uplinkLinkName = config.getString("uplinkLink", "UDP_TC_OUT.vc1");
        this.uplinkChunkSize = config.getInt("uplinkChunkSize", 128);
        this.interPacketDelayMs = config.getLong("interPacketDelayMs", 20L);
        this.fileDownlinkCommandName = config.getString("fileDownlinkCommand", "");
        this.sourceFileNameArg = config.getString("sourceFileNameArg", "sourceFileName");
        this.destFileNameArg = config.getString("destFileNameArg", "destFileName");
        this.downlinkCommandArgs = config.containsKey("downlinkCommandArgs")
                ? config.getMap("downlinkCommandArgs") : Map.of();
        this.listDirectoryCommandName = config.getString("listDirectoryCommand", "");
        this.listDirDirNameArg = config.getString("listDirDirNameArg", "dirName");
        this.eventsStreamName = config.getString("eventsStream", "events_realtime");
        this.downloadTimeoutMs = config.getLong("downloadTimeoutMs", 30000L);
        if (fileApid < 0 || fileApid > SpacePacket.MAX_APID) {
            throw new InitException("fileApid " + fileApid + " outside [0, "
                    + SpacePacket.MAX_APID + "]");
        }

        log.info("FprimeFilePacketService init: inStream={} bucket={} fileApid={}"
                + " uplinkLink={} chunk={}B downlinkMirror={}",
                inStreamName, bucketName, fileApid, uplinkLinkName, uplinkChunkSize,
                downlinkMirrorDir);
    }

    @Override
    protected void addCapabilities(FileTransferCapabilities.Builder b) {
        b.setUpload(true)          // operators can push files to F´
         .setDownload(true)        // operators can pull files from F´
         .setRemotePath(true)      // paths on either side are arbitrary
         .setFileList(true)        // browse F´'s filesystem via the UI
         .setHasTransferType(false)
         .setPauseResume(false);
    }

    // ------------------------------------------------------------------
    // Service lifecycle
    // ------------------------------------------------------------------

    @Override
    protected void doStart() {
        try {
            YarchDatabaseInstance yarch = YarchDatabase.getInstance(yamcsInstance);
            this.inStream = yarch.getStream(inStreamName);
            if (this.inStream == null) {
                notifyFailed(new IllegalStateException("Stream not found: " + inStreamName));
                return;
            }

            Bucket bucket = getOrCreateBucket(bucketName);

            UplinkTransport transport = TcLinkUplinkTransport.resolve(
                    yamcsInstance, uplinkLinkName, getClass().getSimpleName(),
                    interPacketDelayMs);
            this.uplinkHandler = new FilePacketUplinkHandler(
                    transport, fileApid, uplinkChunkSize, transferListener);
            this.storageExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "FprimeFilePacketService-storage");
                t.setDaemon(true);
                return t;
            });
            this.downlinkHandler = new FilePacketDownlinkHandler(
                    bucket, downlinkMirrorDir, maxFileSize,
                    this::resolveDownlinkTransfer, transferListener, storageExecutor);

            // Uplink worker: single-threaded so transfers serialize and the
            // space packet stream stays ordered.
            this.uplinkExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "FprimeFilePacketService-uplink");
                t.setDaemon(true);
                return t;
            });

            // Download timeout sweeper: check every 5 seconds for
            // pending-download transfers that have been waiting too long for
            // F´'s Start packet and flip them to FAILED.
            this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "FprimeFilePacketService-timeout");
                t.setDaemon(true);
                return t;
            });
            this.timeoutScheduler.scheduleWithFixedDelay(
                    this::sweepDownlinkTimeouts, 5, 5, TimeUnit.SECONDS);

            // Spacecraft command synthesis (downlink trigger, file listing).
            if (resolveProcessor()) {
                this.fileDownlinkCommand = findCommand(fileDownlinkCommandName, "SendFile");
                this.listDirectoryCommand = findCommand(listDirectoryCommandName, "ListDirectory");
                if (fileDownlinkCommand == null) {
                    log.warn("File downlink command '{}' not found in MDB; "
                            + "startDownload() will fail", fileDownlinkCommandName);
                }
                if (listDirectoryCommand == null) {
                    log.warn("ListDirectory command '{}' not found in MDB; "
                            + "fetchFileList will fail", listDirectoryCommandName);
                }
            }

            // fprime-yamcs-events publishes decoded F´ events into the
            // events_realtime stream; the listing handler consumes FileManager
            // directory-listing events from it.
            this.eventsStream = yarch.getStream(eventsStreamName);
            if (eventsStream != null) {
                this.eventsSubscriber = listingHandler;
                eventsStream.addSubscriber(eventsSubscriber);
                log.info("Subscribed to {} for remote file listings", eventsStreamName);
            } else {
                log.warn("{} stream not found; fetchFileList will not "
                        + "be able to collect results", eventsStreamName);
            }

            this.inStream.addSubscriber(this);
            log.info("FprimeFilePacketService started: subscribed to {}, "
                    + "ready for file transfers via YAMCS FileTransferService API",
                    inStreamName);
            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        if (timeoutScheduler != null) {
            timeoutScheduler.shutdownNow();
        }
        if (uplinkExecutor != null) {
            uplinkExecutor.shutdownNow();
        }
        if (inStream != null) {
            inStream.removeSubscriber(this);
        }
        if (storageExecutor != null) {
            // Drain in-flight bucket writes before failing leftovers so a
            // finishing store and the shutdown sweep don't race on state.
            storageExecutor.shutdown();
            try {
                storageExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (eventsStream != null && eventsSubscriber != null) {
            eventsStream.removeSubscriber(eventsSubscriber);
        }
        // No packets or uplink work can progress after the executors stop;
        // flip every remaining non-terminal transfer to FAILED so nothing
        // stays QUEUED/RUNNING forever.
        pendingDownloadsByPath.clear();
        failNonTerminalTransfers("service stopped");
        shutdownMonitorNotifier();
        notifyStopped();
    }

    // ------------------------------------------------------------------
    // StreamSubscriber: called for every packet on the TM stream
    // ------------------------------------------------------------------

    @Override
    public void onTuple(Stream stream, Tuple tuple) {
        // Each tm_realtime tuple has a "packet" column with the raw CCSDS bytes.
        Object packetCol = tuple.getColumn("packet");
        if (!(packetCol instanceof byte[])) {
            return;
        }
        byte[] bytes = extractFilePacket((byte[]) packetCol, fileApid);
        if (bytes != null) {
            downlinkHandler.handleFilePacket(bytes, SpacePacket.PRIMARY_HEADER_LEN);
        }
    }

    /**
     * Filter a raw TM packet down to an Fw::FilePacket space packet on
     * {@code apid}, trimmed to its CCSDS-declared length so trailing frame
     * padding is never parsed as packet content. Returns null for non-file
     * packets.
     */
    static byte[] extractFilePacket(byte[] bytes, int apid) {
        if (bytes.length < SpacePacket.PRIMARY_HEADER_LEN + FilePacket.minimumLength()) {
            return null;  // Too short to be a file packet; some other APID.
        }
        if (SpacePacket.apid(bytes) != apid) {
            return null;  // Not a file packet.
        }
        if (!FilePacket.isFilePacket(bytes, SpacePacket.PRIMARY_HEADER_LEN)) {
            return null;  // Right APID but not an Fw::FilePacket descriptor.
        }
        int declared = SpacePacket.declaredLength(bytes);
        if (bytes.length > declared) {
            bytes = Arrays.copyOf(bytes, declared);
        }
        return bytes;
    }

    @Override
    public void streamClosed(Stream stream) {
        log.info("Stream {} closed", stream.getName());
    }

    private FprimeFileTransfer resolveDownlinkTransfer(String sourcePath,
                                                       String destinationPath, int fileSize) {
        return resolveDownlinkTransfer(bucketName, TRANSFER_TYPE,
                sourcePath, destinationPath, fileSize);
    }

    /** Scheduled task: expire stale listings, reassemblies, and pending downloads. */
    private void sweepDownlinkTimeouts() {
        listingHandler.expireStaleListings();
        downlinkHandler.expireInflight(INFLIGHT_STALL_TIMEOUT_MS);
        sweepPendingDownloadTimeouts(downloadTimeoutMs, "Start packet");
    }

    // ------------------------------------------------------------------
    // FileTransferService: upload / download
    // ------------------------------------------------------------------

    @Override
    public FileTransfer startUpload(String sourceEntity, Bucket sourceBucket,
                                    String objectName, String destinationEntity,
                                    String remotePath, TransferOptions options)
            throws IOException {
        return startUploadCommon(sourceBucket, objectName, remotePath, TRANSFER_TYPE,
                maxFileSize, uplinkExecutor, (t, content) -> uplinkHandler.run(t, content));
    }

    @Override
    public FileTransfer startDownload(String sourceEntity, String sourcePath,
                                      String destEntity, Bucket destBucket,
                                      String destPath, TransferOptions options)
            throws IOException {
        // sourceEntity : remote entity name ("spacecraft")
        // sourcePath   : path on the spacecraft to fetch (e.g. "README.md")
        // destEntity   : local entity name ("ground")
        // destBucket   : bucket to deposit the received file in
        // destPath     : bucket object name to store it under
        //
        // Synthesize an F´ FileDownlink.SendFile command with
        // (sourceFileName=sourcePath, destFileName=destPath) and send it
        // through the configured processor. F´ emits Fw::FilePacket frames;
        // the downlink handler reassembles them and writes to the bucket.
        // The two halves are cross-referenced by destPath — the same string
        // appears in the Start packet's destinationPath field.
        return startDownloadCommon(fileDownlinkCommand,
                "Downlink command '" + fileDownlinkCommandName + "' not found in MDB",
                TRANSFER_TYPE, sourcePath, destBucket, destPath,
                sourceFileNameArg, destFileNameArg, downlinkCommandArgs, "Start packet");
    }

    @Override
    public void pause(FileTransfer transfer) {
        throw new UnsupportedOperationException("Pause not supported by Fw::FilePacket");
    }

    @Override
    public void resume(FileTransfer transfer) {
        throw new UnsupportedOperationException("Resume not supported by Fw::FilePacket");
    }

    @Override
    public void cancel(FileTransfer transfer) {
        throw new UnsupportedOperationException(
                "Cancel not supported; Fw::FilePacket transfers are fire-and-forget");
    }

    // ------------------------------------------------------------------
    // Remote file listing trigger
    // ------------------------------------------------------------------

    @Override
    public void fetchFileList(String localEntity, String remoteEntity,
                              String remotePath, Map<String, Object> options) {
        if (listDirectoryCommand == null) {
            // Surface the misconfiguration to the REST/UI caller instead of
            // silently returning and leaving the listing request hanging.
            throw new InvalidRequestException(
                    "Remote file listing unavailable: ListDirectory command not found in MDB");
        }
        String dirName = normalizeDirName(remotePath);
        log.info("fetchFileList: requesting F´ listing of {}", dirName);
        listingHandler.beginListing(dirName);
        try {
            Map<String, Object> args = new HashMap<>();
            args.put(listDirDirNameArg, dirName);
            dispatchCommand(listDirectoryCommand, args,
                    getClass().getSimpleName() + "-listing",
                    (int) (nextTransferId() & 0x7FFFFFFF));
        } catch (Exception e) {
            log.error("fetchFileList({}): failed to dispatch ListDirectory command", dirName, e);
            // Publish a failed listing so the UI isn't stuck.
            listingHandler.failListing(dirName);
        }
    }
}
