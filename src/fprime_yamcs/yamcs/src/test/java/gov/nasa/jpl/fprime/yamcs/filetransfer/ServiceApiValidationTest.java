package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.yamcs.InitException;
import org.yamcs.YConfiguration;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.xtce.MetaCommand;

/**
 * Ground-facing API validation paths of the concrete services: startUpload
 * argument/object checks and the FilePacket fetchFileList error handling.
 * Exercised on init'd (not started) services with an in-memory bucket.
 */
public class ServiceApiValidationTest {

    private static final int MAX = 64;

    private static CfdpFileTransferService cfdpService() throws InitException {
        CfdpFileTransferService svc = new CfdpFileTransferService();
        svc.init("test", "CfdpFileTransferService",
                YConfiguration.wrap(Map.of("maxFileSize", MAX)));
        return svc;
    }

    private static FprimeFilePacketService filePacketService() throws InitException {
        FprimeFilePacketService svc = new FprimeFilePacketService();
        svc.init("test", "FprimeFilePacketService",
                YConfiguration.wrap(Map.of("maxFileSize", MAX)));
        return svc;
    }

    @Test
    public void cfdpStartUploadRejectsBadArguments() throws Exception {
        CfdpFileTransferService svc = cfdpService();
        FakeBucket bucket = new FakeBucket();
        bucket.objects.put("big.bin", new byte[MAX + 1]);

        assertThrows(InvalidRequestException.class,
                () -> svc.startUpload("l", null, "a.bin", "r", "/a", null));
        assertThrows(InvalidRequestException.class,
                () -> svc.startUpload("l", bucket, "", "r", "/a", null));
        assertThrows(InvalidRequestException.class,
                () -> svc.startUpload("l", bucket, "missing.bin", "r", "/a", null));
        assertThrows(InvalidRequestException.class,
                () -> svc.startUpload("l", bucket, "big.bin", "r", "/a", null));
    }

    @Test
    public void filePacketStartUploadRejectsBadArguments() throws Exception {
        FprimeFilePacketService svc = filePacketService();
        FakeBucket bucket = new FakeBucket();
        bucket.objects.put("big.bin", new byte[MAX + 1]);

        assertThrows(InvalidRequestException.class,
                () -> svc.startUpload("l", null, "a.bin", "r", "/a", null));
        assertThrows(InvalidRequestException.class,
                () -> svc.startUpload("l", bucket, "", "r", "/a", null));
        assertThrows(InvalidRequestException.class,
                () -> svc.startUpload("l", bucket, "missing.bin", "r", "/a", null));
        assertThrows(InvalidRequestException.class,
                () -> svc.startUpload("l", bucket, "big.bin", "r", "/a", null));
    }

    @Test
    public void filePacketInitRejectsOutOfRangeApid() {
        FprimeFilePacketService svc = new FprimeFilePacketService();
        assertThrows(InitException.class, () -> svc.init("test", "FprimeFilePacketService",
                YConfiguration.wrap(Map.of("fileApid", 0x0800))));
    }

    @Test
    public void fetchFileListWithoutCommandRejected() throws Exception {
        FprimeFilePacketService svc = filePacketService();
        // No ListDirectory command resolved (service never started).
        assertThrows(InvalidRequestException.class,
                () -> svc.fetchFileList("l", "r", "/logs", Map.of()));
    }

    @Test
    public void fetchFileListDispatchFailureFailsListing() throws Exception {
        FprimeFilePacketService svc = filePacketService();
        svc.listDirectoryCommand = new MetaCommand("ListDirectory");
        // No processor is attached, so command dispatch throws; the listing
        // must be terminated as failed instead of hanging in progress.
        svc.fetchFileList("l", "r", "/logs", Map.of());
        ListFilesResponse listing = svc.getFileList("l", "r", "/logs", Map.of());
        assertEquals("failed", listing.getState());
    }
}
