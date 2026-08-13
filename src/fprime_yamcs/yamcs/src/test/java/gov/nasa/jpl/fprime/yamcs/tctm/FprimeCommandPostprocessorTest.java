package gov.nasa.jpl.fprime.yamcs.tctm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.protobuf.Commanding.CommandId;

import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

public class FprimeCommandPostprocessorTest {

    /** Captures acks and attribute publications. */
    private static final class RecordingPublisher implements CommandHistoryPublisher {
        final List<String> stringValues = new ArrayList<>();
        final List<String> publishedKeys = new ArrayList<>();

        @Override
        public void publish(CommandId id, String key, String value) {
            publishedKeys.add(key);
            stringValues.add(value);
        }

        @Override
        public void publish(CommandId id, String key, int value) {
            publishedKeys.add(key);
        }

        @Override
        public void publish(CommandId id, String key, long value) {
            publishedKeys.add(key);
        }

        @Override
        public void publish(CommandId id, String key, byte[] value) {
            publishedKeys.add(key);
        }

        @Override
        public void addCommand(PreparedCommand pc) {
        }
    }

    private FprimeCommandPostprocessor postprocessor;
    private RecordingPublisher publisher;

    @BeforeEach
    public void setup() {
        postprocessor = new FprimeCommandPostprocessor("test");
        publisher = new RecordingPublisher();
        postprocessor.setCommandHistoryPublisher(publisher);
    }

    private static PreparedCommand command(byte[] binary) {
        PreparedCommand pc = new PreparedCommand(CommandId.newBuilder()
                .setGenerationTime(System.currentTimeMillis())
                .setOrigin("test")
                .setSequenceNumber(1)
                .setCommandName("/test/cmd")
                .build());
        pc.setBinary(binary);
        return pc;
    }

    @Test
    public void rewritesCcsdsLengthField() {
        byte[] binary = new byte[SpacePacket.PRIMARY_HEADER_LEN + 10];
        byte[] result = postprocessor.process(command(binary));

        assertNotNull(result);
        int lengthField = ByteBuffer.wrap(result)
                .getShort(SpacePacket.LENGTH_FIELD_OFFSET) & 0xFFFF;
        assertEquals(10 - 1, lengthField);
        assertTrue(publisher.publishedKeys.contains("ccsds-seqcount"));
        assertTrue(publisher.publishedKeys.contains(PreparedCommand.CNAME_BINARY));

        // Successive commands on the same APID get consecutive 14-bit
        // sequence counts patched into the header.
        int seq1 = ByteBuffer.wrap(result).getShort(2) & 0x3FFF;
        byte[] result2 = postprocessor.process(
                command(new byte[SpacePacket.PRIMARY_HEADER_LEN + 10]));
        int seq2 = ByteBuffer.wrap(result2).getShort(2) & 0x3FFF;
        assertEquals((seq1 + 1) & 0x3FFF, seq2);
    }

    @Test
    public void shortBinaryIsDroppedWithNokAck() {
        byte[] result = postprocessor.process(
                command(new byte[SpacePacket.PRIMARY_HEADER_LEN]));

        assertNull(result);
        assertTrue(publisher.publishedKeys.contains(
                CommandHistoryPublisher.AcknowledgeSent_KEY + "_Status"));
        assertTrue(publisher.stringValues.contains(
                CommandHistoryPublisher.AckStatus.NOK.toString()));
        assertTrue(publisher.stringValues.stream().anyMatch(v -> v.contains("shorter")));
    }

    @Test
    public void oversizeBinaryIsDroppedWithNokAck() {
        byte[] result = postprocessor.process(command(
                new byte[SpacePacket.PRIMARY_HEADER_LEN + SpacePacket.MAX_PAYLOAD_LEN + 1]));

        assertNull(result);
        assertTrue(publisher.publishedKeys.contains(
                CommandHistoryPublisher.AcknowledgeSent_KEY + "_Status"));
        assertTrue(publisher.stringValues.contains(
                CommandHistoryPublisher.AckStatus.NOK.toString()));
        assertTrue(publisher.stringValues.stream().anyMatch(v -> v.contains("exceeds")));
    }
}
