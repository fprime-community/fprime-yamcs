"""fprime_yamcs.comm.bridge: bidirectional data pump between an F Prime endpoint and YAMCS UDP

Runs two threads:

1. Downlink: reads bytes from the F Prime communication adapter (UART, IP, etc.), deframes
   them with the configured framer/deframer plugin, and pushes each resulting packet as a
   UDP datagram to the YAMCS telemetry intake.
2. Uplink: receives UDP datagrams from the YAMCS command outlet, frames each datagram with
   the configured framer/deframer plugin, and writes the result to the communication
   adapter.

With the default no-op framer/deframer, data passes through unmodified in both directions.
"""

import logging
import threading

LOGGER = logging.getLogger("comm_bridge")


class UdpBridge:
    """Bidirectional bridge between a communication adapter and YAMCS UDP endpoints"""

    def __init__(self, adapter, framer, udp):
        """Initialize the bridge

        Args:
            adapter: BaseAdapter instance for the F Prime endpoint side
            framer: FramerDeframer instance used for one stage of framing/deframing
            udp: YamcsUdp instance for the YAMCS side
        """
        self.adapter = adapter
        self.framer = framer
        self.udp = udp
        self.running = True
        self.downlink_thread = threading.Thread(
            target=self.downlink_loop, name="DownlinkThread"
        )
        self.uplink_thread = threading.Thread(
            target=self.uplink_loop, name="UplinkThread"
        )

    def start(self):
        """Open resources and start both data pump threads"""
        self.udp.open()
        self.adapter.open()
        self.downlink_thread.start()
        self.uplink_thread.start()

    def stop(self):
        """Stop the data pump threads and release resources"""
        self.running = False
        self.downlink_thread.join()
        self.uplink_thread.join()
        self.adapter.close()
        self.udp.close()

    def join(self):
        """Wait for both threads to exit"""
        self.downlink_thread.join()
        self.uplink_thread.join()

    def downlink_loop(self):
        """Read from the adapter, deframe, and push packets to the YAMCS UDP intake"""
        pending = b""
        while self.running:
            data = self.adapter.read()
            if not data:
                continue
            pending += data
            packets, pending, discarded = self.framer.deframe_all(pending, no_copy=True)
            if discarded:
                LOGGER.warning("Discarded %d bytes of unframed data", len(discarded))
            for packet in packets:
                self.udp.send(packet)
        LOGGER.debug("Downlink loop exited")

    def uplink_loop(self):
        """Receive datagrams from the YAMCS UDP outlet, frame, and write to the adapter"""
        while self.running:
            datagram = self.udp.receive()
            if not datagram:
                continue
            framed = self.framer.frame(datagram)
            if not self.adapter.write(framed):
                LOGGER.warning("Failed to write %d bytes to adapter", len(framed))
        LOGGER.debug("Uplink loop exited")
