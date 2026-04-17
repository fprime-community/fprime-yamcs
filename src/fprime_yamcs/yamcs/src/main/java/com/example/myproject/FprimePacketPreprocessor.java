package com.example.myproject;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.tctm.AbstractPacketPreprocessor;
import org.yamcs.utils.TimeEncoding;

/**
 * Component capable of modifying packet binary received from a link, before
 * passing it further into Yamcs.
 * <p>
 * A single instance of this class is created, scoped to the link udp-in.
 * <p>
 * This is specified in the configuration file yamcs.myproject.yaml:
 * 
 * <pre>
 * ...
 * dataLinks:
 *   - name: udp-in
 *     class: org.yamcs.tctm.UdpTmDataLink
 *     stream: tm_realtime
 *     host: localhost
 *     port: 10015
 *     packetPreprocessorClassName: com.example.myproject.FprimePacketPreprocessor
 * ...
 * </pre>
 */
public class FprimePacketPreprocessor extends AbstractPacketPreprocessor {

    private Map<Integer, AtomicInteger> seqCounts = new HashMap<>();

    // Widths and offsets
    private static final int SPACE_PACKET_HEADER_LEN = 6;
    private static final int FwPacketDescriptorType_SIZE = 2;
    private static final int FwTlmPacketizeIdType_SIZE = 2;
    private static final int FwTimeBaseStoreType_SIZE = 2;
    private static final int FwTimeContextStoreType_SIZE = 1;
    private static final int FwEventIdType_SIZE = 4;

    private static final int TLM_TIME_TAG_OFFSET = SPACE_PACKET_HEADER_LEN + FwPacketDescriptorType_SIZE
            + FwTlmPacketizeIdType_SIZE + FwTimeBaseStoreType_SIZE + FwTimeContextStoreType_SIZE;

    private static final int EVENT_TIME_TAG_OFFSET = SPACE_PACKET_HEADER_LEN + FwPacketDescriptorType_SIZE
            + FwEventIdType_SIZE + FwTimeBaseStoreType_SIZE + FwTimeContextStoreType_SIZE;

    // APIDs
    private static final int APID_EVENT = 2; // default F' APID for events
    private static final int APID_TLM_PKT = 4; // default F' APID for telemetry packets

    // Constructor used when this preprocessor is used without YAML configuration
    public FprimePacketPreprocessor(String yamcsInstance) {
        this(yamcsInstance, YConfiguration.emptyConfig());
    }

    // Constructor used when this preprocessor is used with YAML configuration
    // (packetPreprocessorClassArgs)
    public FprimePacketPreprocessor(String yamcsInstance, YConfiguration config) {
        super(yamcsInstance, config);
    }

    @Override
    public TmPacket process(TmPacket packet) {

        byte[] bytes = packet.getPacket();

        if (bytes.length < 6) { // Expect at least the length of CCSDS primary header
            eventProducer.sendWarning("SHORT_PACKET",
                    "Short packet received, length: " + bytes.length + "; minimum required length is 6 bytes.");

            // If we return null, the packet is dropped.
            return null;
        }

        // Verify continuity for a given APID based on the CCSDS sequence counter
        int apidseqcount = ByteBuffer.wrap(bytes).getInt(0);
        int apid = (apidseqcount >> 16) & 0x07FF;
        int seq = (apidseqcount) & 0x3FFF;
        AtomicInteger ai = seqCounts.computeIfAbsent(apid, k -> new AtomicInteger());
        int oldseq = ai.getAndSet(seq);

        if (((seq - oldseq) & 0x3FFF) != 1) {
            eventProducer.sendWarning("SEQ_COUNT_JUMP",
                    "Sequence count jump for APID: " + apid + " old seq: " + oldseq + " newseq: " + seq);
        }

        int time_tag_offset = 0;
        // Find time tags depending on APID
        if (apid == APID_EVENT) {
            time_tag_offset = EVENT_TIME_TAG_OFFSET;
        } else if (apid == APID_TLM_PKT) {
            time_tag_offset = TLM_TIME_TAG_OFFSET;
        }
        // Weird stuff with leap seconds, see
        // https://docs.yamcs.org/yamcs-server-manual/general/time/
        int leapSecondsOffset = 38;
        int timeSec = ByteBuffer.wrap(bytes).getInt(time_tag_offset) + leapSecondsOffset;
        int timeUsec = ByteBuffer.wrap(bytes).getInt(time_tag_offset + 4); // sec is 4 bytes width
        long packetGenerationTime = (timeSec * 1000L) + (timeUsec / 1000L);

        // Our custom packets don't include a secundary header with time information.
        // Use Yamcs-local time instead.
        packet.setGenerationTime(packetGenerationTime);

        // Use the full 32-bits, so that both APID and the count are included.
        // Yamcs uses this attribute to uniquely identify the packet (together with the
        // gentime)
        packet.setSequenceCount(apidseqcount);

        return packet;
    }

}