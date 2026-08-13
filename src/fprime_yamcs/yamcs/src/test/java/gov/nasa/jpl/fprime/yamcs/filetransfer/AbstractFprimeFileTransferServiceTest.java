package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.yamcs.buckets.Bucket;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.filetransfer.FileTransferFilter;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.TransferMonitor;
import org.yamcs.filetransfer.TransferOptions;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.protobuf.FileTransferCapabilities;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;
import org.yamcs.xtce.MetaCommand;

public class AbstractFprimeFileTransferServiceTest {

    /** Minimal concrete service exposing the shared bookkeeping for test. */
    private static class TestService extends AbstractFprimeFileTransferService {
        @Override
        protected void addCapabilities(FileTransferCapabilities.Builder builder) {
        }

        @Override
        public void fetchFileList(String source, String destination, String remotePath,
                java.util.Map<String, Object> options) {
        }

        @Override
        public FileTransfer startUpload(String source, Bucket bucket, String objectName,
                String destination, String destinationPath, TransferOptions options) {
            return null;
        }

        @Override
        public FileTransfer startDownload(String source, String sourcePath, String destination,
                Bucket bucket, String objectName, TransferOptions options) {
            return null;
        }

        @Override
        public void pause(FileTransfer transfer) {
        }

        @Override
        public void resume(FileTransfer transfer) {
        }

        @Override
        public void cancel(FileTransfer transfer) {
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }
    }

    private final TestService service = new TestService();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    public void shutdown() {
        executor.shutdownNow();
    }

    private FprimeFileTransfer transfer(TransferDirection direction) {
        return new FprimeFileTransfer(service.nextTransferId(), "bucket", "obj",
                "/remote", 10, direction, "TEST", false);
    }

    @Test
    public void transferIdsAreUniqueAcrossThreads() throws Exception {
        int perThread = 200;
        int threads = 4;
        java.util.Set<Long> ids = java.util.concurrent.ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    for (int j = 0; j < perThread; j++) {
                        ids.add(service.nextTransferId());
                    }
                }));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(threads * perThread, ids.size());
    }

    @Test
    public void addTransferIsRetrievableById() {
        FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
        service.addTransfer(t);
        assertSame(t, service.getFileTransfer(t.getId()));
        assertNull(service.getFileTransfer(999_999L));
    }

    @Test
    public void getTransfersFiltersByDirectionStateAndLimit() {
        FprimeFileTransfer up = transfer(TransferDirection.UPLOAD);
        FprimeFileTransfer down = transfer(TransferDirection.DOWNLOAD);
        down.setState(TransferState.COMPLETED);
        service.addTransfer(up);
        service.addTransfer(down);

        assertEquals(2, service.getTransfers(null).size());

        FileTransferFilter byDirection = new FileTransferFilter();
        byDirection.direction = TransferDirection.DOWNLOAD;
        assertEquals(List.of(down), service.getTransfers(byDirection));

        FileTransferFilter byState = new FileTransferFilter();
        byState.states = List.of(TransferState.COMPLETED);
        assertEquals(List.of(down), service.getTransfers(byState));

        FileTransferFilter limited = new FileTransferFilter();
        limited.limit = 1;
        // Newest first (default descending): the limit keeps the most recent.
        assertEquals(List.of(down), service.getTransfers(limited));

        FileTransferFilter ascending = new FileTransferFilter();
        ascending.descending = false;
        assertEquals(List.of(up, down), service.getTransfers(ascending));
    }

    @Test
    public void getTransfersFiltersByEntityIdsAndTimeWindow() {
        FprimeFileTransfer a = transfer(TransferDirection.UPLOAD);
        a.setEntityIds(1L, 2L);
        FprimeFileTransfer b = transfer(TransferDirection.UPLOAD);
        b.setEntityIds(3L, 4L);
        service.addTransfer(a);
        service.addTransfer(b);

        FileTransferFilter byLocal = new FileTransferFilter();
        byLocal.localEntityId = 1L;
        assertEquals(List.of(a), service.getTransfers(byLocal));

        FileTransferFilter byRemote = new FileTransferFilter();
        byRemote.remoteEntityId = 4L;
        assertEquals(List.of(b), service.getTransfers(byRemote));

        FileTransferFilter futureWindow = new FileTransferFilter();
        futureWindow.start = a.getCreationTime() + 3_600_000L;
        assertEquals(List.of(), service.getTransfers(futureWindow));

        FileTransferFilter pastWindow = new FileTransferFilter();
        pastWindow.stop = a.getCreationTime() - 3_600_000L;
        assertEquals(List.of(), service.getTransfers(pastWindow));

        FileTransferFilter openWindow = new FileTransferFilter();
        openWindow.start = a.getCreationTime() - 3_600_000L;
        openWindow.stop = a.getCreationTime() + 3_600_000L;
        assertEquals(List.of(b, a), service.getTransfers(openWindow));
    }

    @Test
    public void fetchObjectTranslatesAsyncFailuresToIoException() throws Exception {
        FakeBucket bucket = new FakeBucket();
        bucket.objects.put("obj", new byte[] { 1, 2 });
        assertEquals(2, AbstractFprimeFileTransferService.fetchObject(bucket, "obj").length);

        bucket.failGets = true;
        java.io.IOException e = assertThrows(java.io.IOException.class,
                () -> AbstractFprimeFileTransferService.fetchObject(bucket, "obj"));
        assertTrue(e.getMessage().contains("read error"));
    }

    @Test
    public void terminalTransfersEvictedPastHistoryBound() {
        for (int i = 0; i < AbstractFprimeFileTransferService.MAX_TRANSFER_HISTORY + 5; i++) {
            FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
            t.setState(TransferState.COMPLETED);
            service.addTransfer(t);
        }
        assertEquals(AbstractFprimeFileTransferService.MAX_TRANSFER_HISTORY,
                service.getTransfers(null).size());
    }

    @Test
    public void runningTransfersSurviveEviction() {
        List<FprimeFileTransfer> running = new ArrayList<>();
        for (int i = 0; i < AbstractFprimeFileTransferService.MAX_TRANSFER_HISTORY + 5; i++) {
            FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
            if (i < 5) {
                t.setState(TransferState.RUNNING);
                running.add(t);
            } else {
                t.setState(TransferState.COMPLETED);
            }
            service.addTransfer(t);
        }
        for (FprimeFileTransfer t : running) {
            assertSame(t, service.getFileTransfer(t.getId()));
        }
    }

    @Test
    public void failNonTerminalTransfersFlipsOnlyNonTerminal() {
        FprimeFileTransfer queued = transfer(TransferDirection.UPLOAD);
        FprimeFileTransfer done = transfer(TransferDirection.DOWNLOAD);
        done.setState(TransferState.COMPLETED);
        service.addTransfer(queued);
        service.addTransfer(done);

        service.failNonTerminalTransfers("service stopped");

        assertEquals(TransferState.FAILED, queued.getTransferState());
        assertEquals("service stopped", queued.getFailuredReason());
        assertEquals(TransferState.COMPLETED, done.getTransferState());
    }

    @Test
    public void monitorsNotifiedAndThrowingMonitorIsolated() throws Exception {
        // Notifications are dispatched on the service's notifier thread.
        List<FileTransfer> seen = Collections.synchronizedList(new ArrayList<>());
        TransferMonitor bad = t -> {
            throw new IllegalStateException("boom");
        };
        TransferMonitor good = seen::add;
        service.registerTransferMonitor(bad);
        service.registerTransferMonitor(good);

        FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
        service.notifyStateChanged(t);
        awaitSize(seen, 1);
        assertEquals(List.of(t), seen);

        service.unregisterTransferMonitor(bad);
        service.unregisterTransferMonitor(good);
        service.notifyStateChanged(t);
        awaitNotifierIdle();
        assertEquals(1, seen.size());
    }

    private void awaitSize(List<?> list, int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (list.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, list.size());
    }

    /** Push a marker notification through the single-threaded notifier. */
    private void awaitNotifierIdle() throws Exception {
        List<FileTransfer> marker = Collections.synchronizedList(new ArrayList<>());
        TransferMonitor m = marker::add;
        service.registerTransferMonitor(m);
        service.notifyStateChanged(transfer(TransferDirection.UPLOAD));
        awaitSize(marker, 1);
        service.unregisterTransferMonitor(m);
    }

    @Test
    public void failTransferSetsReasonAndState() {
        FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
        service.addTransfer(t);
        service.failTransfer(t, AckStatus.NOK, "no link");
        assertEquals(TransferState.FAILED, t.getTransferState());
        assertEquals("no link", t.getFailuredReason());
    }

    @Test
    public void uplinkBacklogIsBounded() throws Exception {
        Object gate = new Object();
        synchronized (gate) {
            // First task blocks the single worker; the rest sit in the queue.
            for (int i = 0; i < AbstractFprimeFileTransferService.MAX_PENDING_UPLOADS; i++) {
                service.submitUplink(executor, transfer(TransferDirection.UPLOAD), () -> {
                    synchronized (gate) {
                        // released when the test exits the synchronized block
                    }
                });
            }
            FprimeFileTransfer overflow = transfer(TransferDirection.UPLOAD);
            InvalidRequestException e = assertThrows(InvalidRequestException.class,
                    () -> service.submitUplink(executor, overflow, () -> {
                    }));
            assertTrue(e.getMessage().contains("backlog"));
        }
    }

    @Test
    public void releasedUplinkSlotsBecomeAvailableAgain() {
        // Reserve every slot, release them all, then verify a fresh
        // reservation succeeds: failed fetches must not leak slots.
        for (int i = 0; i < AbstractFprimeFileTransferService.MAX_PENDING_UPLOADS; i++) {
            service.reserveUplinkSlot();
        }
        assertThrows(InvalidRequestException.class, () -> service.reserveUplinkSlot());
        for (int i = 0; i < AbstractFprimeFileTransferService.MAX_PENDING_UPLOADS; i++) {
            service.releaseUplinkSlot();
        }
        service.reserveUplinkSlot();
        service.releaseUplinkSlot();
    }

    /** TestService with a fake command dispatcher for the download skeleton. */
    private static final class DispatchTestService extends TestService {
        final List<Map<String, Object>> dispatched = new ArrayList<>();
        boolean failDispatch;

        @Override
        protected CommandId dispatchCommand(MetaCommand command, Map<String, Object> args,
                String origin, int sequenceNumber) throws Exception {
            if (failDispatch) {
                throw new IllegalStateException("no processor");
            }
            dispatched.add(args);
            return CommandId.newBuilder().setOrigin(origin)
                    .setSequenceNumber(sequenceNumber).setGenerationTime(0)
                    .setCommandName("/TEST/cmd").build();
        }
    }

    private static final MetaCommand DOWNLINK_CMD = new MetaCommand("SendFile");

    @Test
    public void startDownloadRegistersPendingAndResolvesIt() throws Exception {
        DispatchTestService svc = new DispatchTestService();
        FakeBucket bucket = new FakeBucket();
        FprimeFileTransfer t = svc.startDownloadCommon(DOWNLINK_CMD, "unavailable",
                "TEST", "/logs/a.bin", bucket, null, "src", "dst", Map.of(), "Start packet");

        // destPath derived from the source basename.
        assertEquals("a.bin", t.getObjectName());
        assertEquals(TransferState.QUEUED, t.getTransferState());
        assertEquals(List.of(Map.of("src", "/logs/a.bin", "dst", "a.bin")), svc.dispatched);

        // The spacecraft's Start/Metadata resolves the pending transfer
        // rather than creating an unsolicited record.
        FprimeFileTransfer resolved = svc.resolveDownlinkTransfer(
                "bucket", "TEST", "/logs/a.bin", "a.bin", 123);
        assertSame(t, resolved);
        assertEquals(123, resolved.getTotalSize());

        // Once consumed, the same name resolves to a fresh unsolicited record.
        FprimeFileTransfer unsolicited = svc.resolveDownlinkTransfer(
                "bucket", "TEST", "/logs/a.bin", "a.bin", 5);
        assertTrue(unsolicited.getId() != t.getId());
    }

    @Test
    public void startDownloadRejectsDuplicatePendingDestination() throws Exception {
        DispatchTestService svc = new DispatchTestService();
        FakeBucket bucket = new FakeBucket();
        svc.startDownloadCommon(DOWNLINK_CMD, "unavailable", "TEST",
                "a.bin", bucket, null, "src", "dst", Map.of(), "Start packet");
        InvalidRequestException e = assertThrows(InvalidRequestException.class,
                () -> svc.startDownloadCommon(DOWNLINK_CMD, "unavailable", "TEST",
                        "other/a.bin", bucket, "a.bin", "src", "dst", Map.of(), "Start packet"));
        assertTrue(e.getMessage().contains("already pending"));
    }

    @Test
    public void startDownloadValidatesInputs() {
        DispatchTestService svc = new DispatchTestService();
        FakeBucket bucket = new FakeBucket();
        assertThrows(InvalidRequestException.class,
                () -> svc.startDownloadCommon(null, "unavailable", "TEST",
                        "a.bin", bucket, null, "src", "dst", Map.of(), "Start packet"));
        assertThrows(InvalidRequestException.class,
                () -> svc.startDownloadCommon(DOWNLINK_CMD, "unavailable", "TEST",
                        "", bucket, null, "src", "dst", Map.of(), "Start packet"));
        assertThrows(InvalidRequestException.class,
                () -> svc.startDownloadCommon(DOWNLINK_CMD, "unavailable", "TEST",
                        "a.bin", null, null, "src", "dst", Map.of(), "Start packet"));
        // Trailing-slash source path yields an empty basename.
        InvalidRequestException e = assertThrows(InvalidRequestException.class,
                () -> svc.startDownloadCommon(DOWNLINK_CMD, "unavailable", "TEST",
                        "/logs/", bucket, null, "src", "dst", Map.of(), "Start packet"));
        assertTrue(e.getMessage().contains("destination file name"));
    }

    @Test
    public void startDownloadRollsBackPendingOnDispatchFailure() throws Exception {
        DispatchTestService svc = new DispatchTestService();
        svc.failDispatch = true;
        FakeBucket bucket = new FakeBucket();
        assertThrows(java.io.IOException.class,
                () -> svc.startDownloadCommon(DOWNLINK_CMD, "unavailable", "TEST",
                        "a.bin", bucket, null, "src", "dst", Map.of(), "Start packet"));
        FileTransfer failed = svc.getTransfers(null).get(0);
        assertEquals(TransferState.FAILED, failed.getTransferState());

        // The pending slot was released, so a retry is not blocked.
        svc.failDispatch = false;
        FprimeFileTransfer retry = svc.startDownloadCommon(DOWNLINK_CMD, "unavailable",
                "TEST", "a.bin", bucket, null, "src", "dst", Map.of(), "Start packet");
        assertEquals(TransferState.QUEUED, retry.getTransferState());
    }

    @Test
    public void startDownloadRejectedWhenPendingCapReached() throws Exception {
        DispatchTestService svc = new DispatchTestService();
        FakeBucket bucket = new FakeBucket();
        for (int i = 0; i < AbstractFprimeFileTransferService.MAX_PENDING_DOWNLOADS; i++) {
            svc.startDownloadCommon(DOWNLINK_CMD, "unavailable", "TEST",
                    "f" + i + ".bin", bucket, null, "src", "dst", Map.of(), "Start packet");
        }
        assertThrows(org.yamcs.filetransfer.InvalidRequestException.class,
                () -> svc.startDownloadCommon(DOWNLINK_CMD, "unavailable", "TEST",
                        "overflow.bin", bucket, null, "src", "dst", Map.of(), "Start packet"));
    }

    @Test
    public void sweepFailsOnlyTimedOutPendingDownloads() throws Exception {
        DispatchTestService svc = new DispatchTestService();
        FakeBucket bucket = new FakeBucket();
        FprimeFileTransfer stale = svc.startDownloadCommon(DOWNLINK_CMD, "unavailable",
                "TEST", "stale.bin", bucket, null, "src", "dst", Map.of(), "Start packet");
        FprimeFileTransfer fresh = svc.startDownloadCommon(DOWNLINK_CMD, "unavailable",
                "TEST", "fresh.bin", bucket, null, "src", "dst", Map.of(), "Start packet");
        stale.setStartTime(System.currentTimeMillis() - 60_000);

        svc.sweepPendingDownloadTimeouts(30_000, "Start packet");

        assertEquals(TransferState.FAILED, stale.getTransferState());
        assertTrue(stale.getFailuredReason().contains("Start packet"));
        assertEquals(TransferState.QUEUED, fresh.getTransferState());
        // The stale entry is gone: its name resolves to a new record now.
        assertTrue(svc.resolveDownlinkTransfer("bucket", "TEST", "s", "stale.bin", 1)
                .getId() != stale.getId());
        assertSame(fresh, svc.resolveDownlinkTransfer("bucket", "TEST", "s", "fresh.bin", 1));
    }

    @Test
    public void uplinkSlotsReleasedAfterCompletion() throws Exception {
        for (int round = 0; round < 3; round++) {
            int submitted = 0;
            long deadline = System.currentTimeMillis() + 5000;
            // Fill the backlog bound in every round: slots freed by completed
            // tasks must become available again (finally-block decrements).
            while (submitted < AbstractFprimeFileTransferService.MAX_PENDING_UPLOADS) {
                try {
                    service.submitUplink(executor, transfer(TransferDirection.UPLOAD), () -> {
                    });
                    submitted++;
                } catch (InvalidRequestException e) {
                    assertTrue(System.currentTimeMillis() < deadline,
                            "uplink slots never released");
                    Thread.sleep(5);
                }
            }
            executor.submit(() -> {
            }).get();
        }
    }
}
