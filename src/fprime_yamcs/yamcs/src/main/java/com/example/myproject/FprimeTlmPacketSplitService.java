package com.example.myproject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.AbstractYamcsService;
import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

/**
 * Splits aggregated F Prime telemetry channel packets (Fw::TlmPacket, APID 1 by
 * default) into single-entry CCSDS packets so that every batched channel entry is
 * decoded by the XTCE containers (which match one channel per packet).
 *
 * <p>Subscribes to a raw TM stream (fed directly by the data link) and re-emits onto
 * the stream consumed by the realtime processor and the recorders. Non-telemetry
 * packets, single-entry packets, and packets that cannot be walked are forwarded
 * unchanged.
 *
 * <p>Configured in yamcs.[instance].yaml:
 *
 * <pre>
 * services:
 *   - class: com.example.myproject.FprimeTlmPacketSplitService
 *     args:
 *       inStream: tm_raw
 *       outStream: tm_realtime
 *       tlmApid: 1
 *       channelValueSizes:     # channel id -> serialized value size in bytes
 *         "100": 4             # (-1: string, walked via 2-byte length prefix;
 *         "101": -1            #   0: unknown/variable, packet forwarded unsplit)
 *       doNotArchiveChannelIds: [102, 103]
 * </pre>
 *
 * The channelValueSizes map is generated from the F Prime JSON dictionary by
 * fprime-yamcs (see construct_temporary_configuration in __main__.py).
 */
public class FprimeTlmPacketSplitService extends AbstractYamcsService implements StreamSubscriber {

    private static final Logger log = LoggerFactory.getLogger(FprimeTlmPacketSplitService.class);

    private static final int SPACE_PACKET_HEADER_LEN = 6;
    private static final int PACKET_DESCRIPTOR_SIZE = 2;
    private static final int MIN_ENTRY_SIZE = 4 + 11; // channel id + F Prime time tag

    private String inStreamName;
    private String outStreamName;
    private int tlmApid;
    private Stream inStream;
    private Stream outStream;
    private TlmPacketSplitter splitter;
    private final Map<Long, Integer> channelValueSizes = new HashMap<>();
    private final Set<Long> doNotArchiveChannelIds = new HashSet<>();

    @Override
    public Spec getSpec() {
        Spec spec = new Spec();
        spec.addOption("inStream", OptionType.STRING).withDefault("tm_raw");
        spec.addOption("outStream", OptionType.STRING).withDefault("tm_realtime");
        spec.addOption("tlmApid", OptionType.INTEGER).withDefault(1);
        spec.addOption("channelValueSizes", OptionType.MAP).withSpec(Spec.ANY);
        spec.addOption("doNotArchiveChannelIds", OptionType.LIST).withElementType(OptionType.INTEGER);
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config) throws InitException {
        super.init(yamcsInstance, serviceName, config);
        inStreamName = config.getString("inStream", "tm_raw");
        outStreamName = config.getString("outStream", "tm_realtime");
        tlmApid = config.getInt("tlmApid", 1);
        if (config.containsKey("channelValueSizes")) {
            for (Map.Entry<?, ?> entry : config.getMap("channelValueSizes").entrySet()) {
                channelValueSizes.put(Long.parseLong(String.valueOf(entry.getKey())),
                        ((Number) entry.getValue()).intValue());
            }
        }
        if (config.containsKey("doNotArchiveChannelIds")) {
            for (Object id : config.getList("doNotArchiveChannelIds")) {
                doNotArchiveChannelIds.add(((Number) id).longValue());
            }
        }
        splitter = new TlmPacketSplitter(channelValueSizes, doNotArchiveChannelIds);
    }

    @Override
    protected void doStart() {
        YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);
        inStream = ydb.getStream(inStreamName);
        outStream = ydb.getStream(outStreamName);
        if (inStream == null) {
            notifyFailed(new IllegalStateException("Cannot find input stream '" + inStreamName + "'"));
            return;
        }
        if (outStream == null) {
            notifyFailed(new IllegalStateException("Cannot find output stream '" + outStreamName + "'"));
            return;
        }
        inStream.addSubscriber(this);
        log.info("Splitting aggregated F Prime telemetry packets (APID {}) from '{}' onto '{}' ({} channel sizes)",
                tlmApid, inStreamName, outStreamName, channelValueSizes.size());
        notifyStarted();
    }

    @Override
    protected void doStop() {
        if (inStream != null) {
            inStream.removeSubscriber(this);
        }
        notifyStopped();
    }

    @Override
    public void onTuple(Stream stream, Tuple tuple) {
        byte[] packet = tuple.getColumn(StandardTupleDefinitions.TM_PACKET_COLUMN);
        if (packet == null || packet.length < SPACE_PACKET_HEADER_LEN) {
            outStream.emitTuple(tuple);
            return;
        }
        int apid = (ByteBuffer.wrap(packet).getShort(0) & 0x07FF);
        if (apid != tlmApid || isSingleEntry(packet)) {
            outStream.emitTuple(tuple);
            return;
        }
        try {
            for (TlmPacketSplitter.SplitEntry entry : splitter.split(packet)) {
                outStream.emitTuple(makeTuple(tuple, entry));
            }
        } catch (TlmPacketSplitter.SplitException e) {
            log.warn("Failed to split aggregated telemetry packet; forwarding unsplit: {}", e.getMessage());
            outStream.emitTuple(tuple);
        }
    }

    /**
     * Fast path check: a packet whose sole entry has a known fixed size that exactly fills the
     * packet needs no splitting or repackaging.
     */
    private boolean isSingleEntry(byte[] packet) {
        int entriesLength = packet.length - SPACE_PACKET_HEADER_LEN - PACKET_DESCRIPTOR_SIZE;
        if (entriesLength < MIN_ENTRY_SIZE) {
            return true; // too short to walk; forward as-is
        }
        long channelId = ByteBuffer.wrap(packet).getInt(SPACE_PACKET_HEADER_LEN + PACKET_DESCRIPTOR_SIZE)
                & 0xFFFFFFFFL;
        Integer size = channelValueSizes.get(channelId);
        return size != null && size > 0 && entriesLength == MIN_ENTRY_SIZE + size;
    }

    private Tuple makeTuple(Tuple original, TlmPacketSplitter.SplitEntry entry) {
        Tuple tuple = new Tuple(original.getDefinition(), new ArrayList<Object>(original.getColumns()));
        tuple.setColumn(StandardTupleDefinitions.GENTIME_COLUMN, entry.generationTime);
        tuple.setColumn(StandardTupleDefinitions.SEQNUM_COLUMN, entry.apidSeqCount);
        tuple.setColumn(StandardTupleDefinitions.TM_PACKET_COLUMN, entry.packet);
        Integer status = original.getColumn(StandardTupleDefinitions.TM_STATUS_COLUMN);
        int newStatus = (status == null ? 0 : status) & ~TmPacket.STATUS_MASK_DO_NOT_ARCHIVE;
        if (entry.doNotArchive) {
            newStatus |= TmPacket.STATUS_MASK_DO_NOT_ARCHIVE;
        }
        tuple.setColumn(StandardTupleDefinitions.TM_STATUS_COLUMN, newStatus);
        return tuple;
    }

    @Override
    public void streamClosed(Stream stream) {
        // nothing to do
    }
}
