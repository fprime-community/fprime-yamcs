package com.example.myproject;

import java.util.HashMap;
import java.util.Map;

import org.yamcs.YConfiguration;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.tctm.CommandPostprocessor;
import org.yamcs.utils.ByteArrayUtils;

/**
 * Component capable of modifying command binary before passing it to the link
 * for further dispatch.
 * <p>
 * A single instance of this class is created, scoped to the link udp-out.
 * <p>
 * This is specified in the configuration file yamcs.myproject.yaml:
 * 
 * <pre>
 * ...
 * dataLinks:
 *   - name: udp-out
 *     class: org.yamcs.tctm.UdpTcDataLink
 *     stream: tc_realtime
 *     host: localhost
 *     port: 10025
 *     commandPostprocessorClassName: com.example.myproject.FprimeCommandPostprocessor
 * ...
 * </pre>
 */
public class FprimeCommandPostprocessor implements CommandPostprocessor {

    // Per-APID CCSDS sequence counters, starting at 0 to match F Prime's
    // ApidManager expectation (Yamcs' CcsdsSeqCountFiller starts at 1).
    private static final Map<Integer, Integer> seqCounts = new HashMap<>();
    private CommandHistoryPublisher commandHistory;

    // Constructor used when this postprocessor is used without YAML configuration
    public FprimeCommandPostprocessor(String yamcsInstance) {
        this(yamcsInstance, YConfiguration.emptyConfig());
    }

    // Constructor used when this postprocessor is used with YAML configuration
    // (commandPostprocessorClassArgs)
    public FprimeCommandPostprocessor(String yamcsInstance, YConfiguration config) {
    }

    // Called by Yamcs during initialization
    @Override
    public void setCommandHistoryPublisher(CommandHistoryPublisher commandHistory) {
        this.commandHistory = commandHistory;
    }

    // Called by Yamcs *after* a command was submitted, but *before* the link
    // handles it.
    // This method must return the (possibly modified) packet binary.
    @Override
    public byte[] process(PreparedCommand pc) {
        byte[] binary = pc.getBinary();

        // Set CCSDS packet length
        int SPACE_PACKET_HEADER_LEN = 6;
        int SPACE_PACKET_LENGTH_TOKEN_OFFSET = 4;
        // Minus one as per SPP protocol
        ByteArrayUtils.encodeUnsignedShort(binary.length - SPACE_PACKET_HEADER_LEN - 1, binary,
                SPACE_PACKET_LENGTH_TOKEN_OFFSET);

        // Set CCSDS sequence count
        int seqCount = fillSeqCount(binary);

        // Publish the sequence count to Command History. This has no special
        // meaning to Yamcs, but it shows how to store custom information specific
        // to a command.
        commandHistory.publish(pc.getCommandId(), "ccsds-seqcount", seqCount);

        // Since we modified the binary, update the binary in Command History too.
        commandHistory.publish(pc.getCommandId(), PreparedCommand.CNAME_BINARY, binary);

        return binary;
    }

    /** Patches the next per-APID sequence count into the packet and returns it. */
    private static synchronized int fillSeqCount(byte[] binary) {
        int apid = ByteArrayUtils.decodeUnsignedShort(binary, 0) & 0x07FF;
        int seqCount = seqCounts.getOrDefault(apid, 0);
        seqCounts.put(apid, (seqCount + 1) % (1 << 14));
        int seqCtrl = (ByteArrayUtils.decodeUnsignedShort(binary, 2) & 0xC000) | seqCount;
        ByteArrayUtils.encodeUnsignedShort(seqCtrl, binary, 2);
        return seqCount;
    }
}