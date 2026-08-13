package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.yamcs.YConfiguration;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.tctm.TcDataLink;

public class TcLinkUplinkTransportTest {

    /** Minimal TcDataLink stub capturing submitted commands. */
    private static final class FakeTcLink implements TcDataLink {
        final List<PreparedCommand> sent = new ArrayList<>();
        boolean accept = true;

        @Override
        public boolean sendCommand(PreparedCommand pc) {
            if (!accept) {
                return false;
            }
            sent.add(pc);
            return true;
        }

        @Override
        public void setCommandHistoryPublisher(CommandHistoryPublisher chp) {
        }

        @Override
        public Status getLinkStatus() {
            return Status.OK;
        }

        @Override
        public void enable() {
        }

        @Override
        public void disable() {
        }

        @Override
        public boolean isDisabled() {
            return false;
        }

        @Override
        public long getDataInCount() {
            return 0;
        }

        @Override
        public long getDataOutCount() {
            return 0;
        }

        @Override
        public void resetCounters() {
        }

        @Override
        public String getName() {
            return "fake";
        }

        @Override
        public YConfiguration getConfig() {
            return YConfiguration.emptyConfig();
        }
    }

    @Test
    public void sendsCommandWithPopulatedCommandId() throws Exception {
        FakeTcLink link = new FakeTcLink();
        TcLinkUplinkTransport transport = new TcLinkUplinkTransport(link, "TestOrigin", 0);
        byte[] packet = new byte[] { 1, 2, 3, 4, 5, 6, 7 };

        transport.send(packet);

        assertEquals(1, link.sent.size());
        PreparedCommand pc = link.sent.get(0);
        assertArrayEquals(packet, pc.getBinary());
        assertEquals("TestOrigin", pc.getCommandId().getOrigin());
        assertTrue(pc.getCommandId().getGenerationTime() > 0,
                "CommandId generation time must be populated");
    }

    @Test
    public void sequenceNumberIncrementsPerPacket() throws Exception {
        FakeTcLink link = new FakeTcLink();
        TcLinkUplinkTransport transport = new TcLinkUplinkTransport(link, "TestOrigin", 0);

        transport.send(new byte[] { 1 });
        transport.send(new byte[] { 2 });
        transport.send(new byte[] { 3 });

        assertEquals(0, link.sent.get(0).getCommandId().getSequenceNumber());
        assertEquals(1, link.sent.get(1).getCommandId().getSequenceNumber());
        assertEquals(2, link.sent.get(2).getCommandId().getSequenceNumber());
    }

    @Test
    public void interPacketDelayPacesSends() throws Exception {
        FakeTcLink link = new FakeTcLink();
        TcLinkUplinkTransport transport = new TcLinkUplinkTransport(link, "TestOrigin", 25);

        long start = System.nanoTime();
        transport.send(new byte[] { 1 });
        transport.send(new byte[] { 2 });
        transport.send(new byte[] { 3 });
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(3, link.sent.size());
        // Two 25 ms delays; tolerance for Thread.sleep sub-ms rounding.
        assertTrue(elapsedMs >= 45, "expected >= 45 ms of pacing, got " + elapsedMs);
    }

    @Test
    public void pacingIsSharedAcrossTransportsOnTheSameLink() throws Exception {
        FakeTcLink link = new FakeTcLink();
        // Two services (e.g. FilePacket and CFDP) resolving the same link
        // must not interleave packets faster than the drain interval.
        TcLinkUplinkTransport a = new TcLinkUplinkTransport(link, "ServiceA", 25);
        TcLinkUplinkTransport b = new TcLinkUplinkTransport(link, "ServiceB", 25);

        long start = System.nanoTime();
        a.send(new byte[] { 1 });
        b.send(new byte[] { 2 });
        a.send(new byte[] { 3 });
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(3, link.sent.size());
        assertTrue(elapsedMs >= 45, "expected >= 45 ms of cross-service pacing, got " + elapsedMs);
    }

    @Test
    public void linkRejectionPropagatesAsException() {
        FakeTcLink link = new FakeTcLink();
        link.accept = false;
        TcLinkUplinkTransport transport = new TcLinkUplinkTransport(link, "TestOrigin", 0);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> transport.send(new byte[] { 1 }));
        assertTrue(e.getMessage().contains("rejected"));
    }
}
