package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;

public class FprimeFileTransferTest {

    private static FprimeFileTransfer transfer() {
        return new FprimeFileTransfer(1, "bucket", "obj", "/remote", 10,
                TransferDirection.UPLOAD, "TEST", false);
    }

    @Test
    public void statesProgressNormally() {
        FprimeFileTransfer t = transfer();
        assertEquals(TransferState.QUEUED, t.getTransferState());
        assertTrue(t.setState(TransferState.RUNNING));
        assertTrue(t.setState(TransferState.COMPLETED));
        assertEquals(TransferState.COMPLETED, t.getTransferState());
    }

    @Test
    public void terminalStatesAreSticky() {
        FprimeFileTransfer t = transfer();
        assertTrue(t.setState(TransferState.FAILED));
        assertFalse(t.setState(TransferState.COMPLETED),
                "a late completion must not resurrect a failed transfer");
        assertEquals(TransferState.FAILED, t.getTransferState());

        FprimeFileTransfer u = transfer();
        assertTrue(u.setState(TransferState.COMPLETED));
        assertFalse(u.setState(TransferState.FAILED));
        assertEquals(TransferState.COMPLETED, u.getTransferState());
    }

    @Test
    public void terminalStateIsIdempotent() {
        FprimeFileTransfer t = transfer();
        assertTrue(t.setState(TransferState.FAILED));
        assertTrue(t.setState(TransferState.FAILED));
    }

    @Test
    public void entityIdsDefaultAndOverride() {
        FprimeFileTransfer t = transfer();
        assertEquals(FprimeFileTransfer.GROUND_ENTITY_ID, t.getLocalEntityId());
        assertEquals(FprimeFileTransfer.SPACECRAFT_ENTITY_ID, t.getRemoteEntityId());
        t.setEntityIds(7, 9);
        assertEquals(7L, t.getLocalEntityId());
        assertEquals(9L, t.getRemoteEntityId());
    }

    @Test
    public void suffixCandidatesMatchOnSegmentBoundary() {
        List<String> names = List.of(
                "/FileDownlink/SendFile",
                "/Other/AbortSendFile",
                "/Alt.SendFile");
        assertEquals(List.of("/FileDownlink/SendFile", "/Alt.SendFile"),
                AbstractFprimeFileTransferService.suffixCandidates(names, "SendFile"));
        assertEquals(List.of("/Other/AbortSendFile"),
                AbstractFprimeFileTransferService.suffixCandidates(names, "AbortSendFile"));
        assertTrue(AbstractFprimeFileTransferService.suffixCandidates(names, "Nope").isEmpty());
    }
}
