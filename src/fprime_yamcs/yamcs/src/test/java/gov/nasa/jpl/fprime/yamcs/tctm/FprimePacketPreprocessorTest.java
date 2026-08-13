package gov.nasa.jpl.fprime.yamcs.tctm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.events.EventProducerFactory;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.yarch.protobuf.Db.Event;

public class FprimePacketPreprocessorTest {

    @BeforeAll
    public static void setupTimeEncoding() {
        TimeEncoding.setUp();
    }

    private static final int APID_EVENT = 2;
    private static final int APID_TLM_PKT = 4;
    private static final int APID_OTHER = 9;
    // SpacePacket.PRIMARY_HEADER_LEN + descriptor(2) + eventId(4)
    // + timeBase(2) + timeContext(1)
    private static final int EVENT_TIME_TAG_OFFSET = 15;
    // SpacePacket.PRIMARY_HEADER_LEN + descriptor(2) + packetizeId(2)
    // + timeBase(2) + timeContext(1)
    private static final int TLM_TIME_TAG_OFFSET = 13;

    private FprimePacketPreprocessor preprocessor;
    private Queue<Event> events;

    @BeforeEach
    public void setup() {
        EventProducerFactory.setMockup(true);
        events = EventProducerFactory.getMockupQueue();
        events.clear();
        preprocessor = new FprimePacketPreprocessor("test");
    }

    private static byte[] packet(int apid, int seq, int length) {
        byte[] bytes = new byte[length];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putShort((short) apid);
        bb.putShort((short) (0xC000 | (seq & 0x3FFF)));
        bb.putShort((short) (length - 6 - 1));
        return bytes;
    }

    private TmPacket process(byte[] bytes) {
        return preprocessor.process(new TmPacket(System.currentTimeMillis(), bytes));
    }

    @Test
    public void shortPacketIsDroppedWithWarning() {
        assertNull(process(new byte[] { 1, 2, 3 }));
        assertEquals(1, events.size());
        assertTrue(events.peek().getMessage().contains("Short packet"));
    }

    @Test
    public void firstPacketOnApidRaisesNoJumpWarning() {
        assertNotNull(process(packet(APID_OTHER, 100, 20)));
        assertEquals(0, events.size());
    }

    @Test
    public void continuousSequenceRaisesNoWarning() {
        process(packet(APID_OTHER, 100, 20));
        process(packet(APID_OTHER, 101, 20));
        process(packet(APID_OTHER, 102, 20));
        assertEquals(0, events.size());
    }

    @Test
    public void sequenceJumpRaisesWarning() {
        process(packet(APID_OTHER, 100, 20));
        process(packet(APID_OTHER, 105, 20));
        assertEquals(1, events.size());
        assertTrue(events.peek().getMessage().contains("Sequence count jump"));
    }

    @Test
    public void sequenceContinuityIsPerApid() {
        process(packet(APID_OTHER, 100, 20));
        process(packet(APID_OTHER + 1, 500, 20));
        process(packet(APID_OTHER, 101, 20));
        process(packet(APID_OTHER + 1, 501, 20));
        assertEquals(0, events.size());
    }

    @Test
    public void fourteenBitSequenceWrapIsContinuous() {
        process(packet(APID_OTHER, 0x3FFF, 20));
        process(packet(APID_OTHER, 0, 20));
        assertEquals(0, events.size());
    }

    @Test
    public void remappedEventApidHonoredViaConfig() {
        preprocessor = new FprimePacketPreprocessor("test",
                YConfiguration.wrap(Map.of("eventApid", 7)));
        long fprimeSeconds = 1_000_000L;
        byte[] bytes = packet(7, 1, EVENT_TIME_TAG_OFFSET + 8);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putInt(EVENT_TIME_TAG_OFFSET, (int) fprimeSeconds);
        bb.putInt(EVENT_TIME_TAG_OFFSET + 4, 500_000); // microseconds

        TmPacket result = process(bytes);
        assertEquals(TimeEncoding.fromUnixMillisec(fprimeSeconds * 1000L + 500L),
                result.getGenerationTime());
    }

    @Test
    public void remappedTlmPktApidHonoredViaConfig() {
        preprocessor = new FprimePacketPreprocessor("test",
                YConfiguration.wrap(Map.of("tlmPktApid", 8)));
        long fprimeSeconds = 2_000_000L;
        byte[] bytes = packet(8, 1, TLM_TIME_TAG_OFFSET + 8);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putInt(TLM_TIME_TAG_OFFSET, (int) fprimeSeconds);
        bb.putInt(TLM_TIME_TAG_OFFSET + 4, 250_000); // microseconds

        TmPacket result = process(bytes);
        assertEquals(TimeEncoding.fromUnixMillisec(fprimeSeconds * 1000L + 250L),
                result.getGenerationTime());
    }

    @Test
    public void eventPacketTimeTagBecomesGenerationTime() {
        long fprimeSeconds = 1_000_000L;
        byte[] bytes = packet(APID_EVENT, 1, EVENT_TIME_TAG_OFFSET + 8);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putInt(EVENT_TIME_TAG_OFFSET, (int) fprimeSeconds);
        bb.putInt(EVENT_TIME_TAG_OFFSET + 4, 500_000); // microseconds

        TmPacket result = process(bytes);
        // YAMCS's TAI-UTC leap-second table converts the Unix time tag.
        assertEquals(TimeEncoding.fromUnixMillisec(fprimeSeconds * 1000L + 500L),
                result.getGenerationTime());
    }

    @Test
    public void telemetryPacketTimeTagBecomesGenerationTime() {
        long fprimeSeconds = 2_000_000L;
        byte[] bytes = packet(APID_TLM_PKT, 1, TLM_TIME_TAG_OFFSET + 8);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putInt(TLM_TIME_TAG_OFFSET, (int) fprimeSeconds);
        bb.putInt(TLM_TIME_TAG_OFFSET + 4, 250_000); // microseconds

        TmPacket result = process(bytes);
        assertEquals(TimeEncoding.fromUnixMillisec(fprimeSeconds * 1000L + 250L),
                result.getGenerationTime());
    }

    @Test
    public void unsignedU32SecondsDoNotWrapNegative() {
        // Past 2038-01-19: a signed 32-bit interpretation would be negative.
        long fprimeSeconds = 0xF0000000L;
        byte[] bytes = packet(APID_EVENT, 1, EVENT_TIME_TAG_OFFSET + 8);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putInt(EVENT_TIME_TAG_OFFSET, (int) fprimeSeconds);
        bb.putInt(EVENT_TIME_TAG_OFFSET + 4, 0);

        TmPacket result = process(bytes);
        assertEquals(TimeEncoding.fromUnixMillisec(fprimeSeconds * 1000L),
                result.getGenerationTime());
    }

    @Test
    public void eventPacketTooShortForTimeTagUsesLocalTime() {
        long before = System.currentTimeMillis();
        TmPacket result = process(packet(APID_EVENT, 1, EVENT_TIME_TAG_OFFSET + 4));
        assertTrue(result.getGenerationTime() >= before);
        assertEquals(1, events.size());
        assertTrue(events.peek().getMessage().contains("too short for time tag"));
    }

    @Test
    public void unknownApidUsesLocalTimeWithoutWarning() {
        long before = System.currentTimeMillis();
        TmPacket result = process(packet(APID_OTHER, 1, 30));
        assertTrue(result.getGenerationTime() >= before);
        assertEquals(0, events.size());
    }

    @Test
    public void sequenceCountAttributeCombinesApidAndSequence() {
        byte[] bytes = packet(APID_OTHER, 7, 20);
        TmPacket result = process(bytes);
        assertEquals(ByteBuffer.wrap(bytes).getInt(0), result.getSeqCount());
    }
}
