package gov.nasa.jpl.fprime.yamcs.filetransfer;

/**
 * Transport used to uplink CCSDS space packets to the spacecraft.
 *
 * <p>Abstracting the transport decouples file transfer services from the
 * uplink pipeline configured in YAMCS: the same service works whether the
 * deployment uses a CCSDS TC frame link (TM/TC pipeline), a raw space packet
 * link, or any other {@code TcDataLink} implementation.
 */
public interface UplinkTransport {

    /**
     * Send one complete CCSDS space packet toward the spacecraft.
     *
     * @param spacePacket a fully formed space packet (primary header + data)
     * @throws Exception if the packet could not be queued or sent
     */
    void send(byte[] spacePacket) throws Exception;
}
