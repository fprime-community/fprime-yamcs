package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.util.ArrayList;
import java.util.List;

import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;

/** Records transfer events for handler unit tests. */
class RecordingListener implements TransferEventListener {

    final List<FprimeFileTransfer> stateChanges = new ArrayList<>();
    final List<AckStatus> acks = new ArrayList<>();
    final List<String> ackMessages = new ArrayList<>();

    @Override
    public void stateChanged(FprimeFileTransfer transfer) {
        stateChanges.add(transfer);
    }

    @Override
    public void verifierAck(FprimeFileTransfer transfer, AckStatus status, String message) {
        acks.add(status);
        ackMessages.add(message);
    }
}
