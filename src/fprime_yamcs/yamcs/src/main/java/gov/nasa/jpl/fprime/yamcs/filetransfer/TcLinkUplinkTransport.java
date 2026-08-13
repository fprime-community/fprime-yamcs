package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.YamcsServer;
import org.yamcs.YamcsServerInstance;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.tctm.Link;
import org.yamcs.tctm.TcDataLink;

/**
 * {@link UplinkTransport} that routes space packets through a YAMCS-configured
 * TC data link as synthetic {@link PreparedCommand}s.
 *
 * <p>Any {@link TcDataLink} works: a CCSDS TC frame virtual channel
 * (e.g. {@code UDP_TC_OUT.vc1}, the TM/TC pipeline) or a raw packet link
 * (e.g. {@code org.yamcs.tctm.UdpTcDataLink} for a space-packet-only uplink).
 * The link runs its configured command postprocessor — which patches the
 * space packet length and sequence count in place — and emits the result via
 * its configured framing and transport.
 */
public class TcLinkUplinkTransport implements UplinkTransport {

    private static final Logger LOG = LoggerFactory.getLogger(TcLinkUplinkTransport.class);

    // One pacer per underlying TcDataLink, shared by every transport that
    // resolves the same link: services (e.g. FilePacket and CFDP) uplinking
    // through one link must not interleave packets faster than the
    // spacecraft-side accumulator drain interval. Weak keys let the pacer
    // die with the link.
    private static final Map<TcDataLink, LinkPacer> PACERS =
            Collections.synchronizedMap(new WeakHashMap<>());

    @FunctionalInterface
    private interface SendAction {
        void run() throws Exception;
    }

    private static final class LinkPacer {
        private long nextSendAtMs;

        // Serializing concurrent senders behind the sleeping monitor is the
        // point: they must not send until the reserved interval elapses.
        // The send itself runs inside the monitor so two senders sharing
        // the link cannot pace, get preempted, and then send back-to-back.
        synchronized void paceAndSend(long delayMs, SendAction action) throws Exception {
            long wait = nextSendAtMs - System.currentTimeMillis();
            if (wait > 0) {
                Thread.sleep(wait);
            }
            try {
                action.run();
            } finally {
                nextSendAtMs = System.currentTimeMillis() + delayMs;
            }
        }
    }

    private final TcDataLink link;
    private final LinkPacer pacer;
    private final String origin;
    private final long interPacketDelayMs;
    // Synthetic CommandId sequence counter. Each uplinked packet gets a
    // unique sequenceNumber so command history distinguishes them.
    private int commandSequence = 0;

    /**
     * Resolve a named YAMCS data link to a {@link TcDataLink} transport.
     *
     * @param yamcsInstance      the YAMCS instance the link belongs to
     * @param linkName           name of the link (e.g. {@code UDP_TC_OUT.vc1})
     * @param origin             origin string stamped on synthetic commands
     * @param interPacketDelayMs pacing delay between packets, in milliseconds
     * @throws IllegalStateException if the link is missing or not a TcDataLink
     */
    public static TcLinkUplinkTransport resolve(String yamcsInstance, String linkName,
                                                String origin, long interPacketDelayMs) {
        if (linkName == null || linkName.isEmpty()) {
            throw new IllegalStateException("uplinkLink config option is required");
        }
        YamcsServerInstance instance = YamcsServer.getServer().getInstance(yamcsInstance);
        if (instance == null) {
            throw new IllegalStateException("YAMCS instance '" + yamcsInstance + "' not found");
        }
        Link link = instance.getLinkManager().getLink(linkName);
        if (!(link instanceof TcDataLink)) {
            String what = link == null ? "not found"
                    : "is " + link.getClass().getSimpleName() + ", not a TcDataLink";
            throw new IllegalStateException("Uplink link '" + linkName + "' " + what);
        }
        LOG.info("Uplink will route through YAMCS link {} ({})",
                linkName, link.getClass().getSimpleName());
        return new TcLinkUplinkTransport((TcDataLink) link, origin, interPacketDelayMs);
    }

    public TcLinkUplinkTransport(TcDataLink link, String origin, long interPacketDelayMs) {
        this.link = link;
        this.pacer = PACERS.computeIfAbsent(link, l -> new LinkPacer());
        this.origin = origin;
        this.interPacketDelayMs = interPacketDelayMs;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The synthetic {@link PreparedCommand} MUST carry a populated
     * {@link CommandId} — the plain {@code PreparedCommand(byte[])}
     * constructor leaves it null, and frame multiplexers call
     * {@code getGenerationTime()} on the queued command from a background
     * thread and NPE otherwise. The command name field is a free-form string
     * as far as {@code sendCommand()} is concerned; dictionary lookup only
     * happens on the stream-tuple round-trip path, which this bypasses.
     */
    @Override
    public void send(byte[] spacePacket) throws Exception {
        PreparedCommand pc;
        synchronized (this) {
            CommandId cmdId = CommandId.newBuilder()
                    .setGenerationTime(System.currentTimeMillis())
                    .setOrigin(origin)
                    .setSequenceNumber(commandSequence++)
                    .setCommandName(origin + "/uplinkPacket")
                    .build();
            pc = new PreparedCommand(cmdId);
            pc.setBinary(spacePacket);
        }
        // Give the spacecraft-side accumulator a moment to drain between
        // packets — across every service sharing this link, not just this
        // transport instance. Pacing and sending are atomic on the shared
        // per-link pacer so the spacing invariant holds across services.
        if (interPacketDelayMs > 0) {
            pacer.paceAndSend(interPacketDelayMs, () -> doSend(pc));
        } else {
            doSend(pc);
        }
    }

    private void doSend(PreparedCommand pc) {
        if (!link.sendCommand(pc)) {
            throw new IllegalStateException("Link rejected packet (queue full or disabled)");
        }
    }
}
