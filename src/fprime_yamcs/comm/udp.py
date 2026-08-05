"""fprime_yamcs.comm.udp: UDP endpoints for exchanging packets with YAMCS

Implements the YAMCS-facing side of the fprime-yamcs-comm bridge. Telemetry (downlink)
packets are pushed as UDP datagrams to the YAMCS UDP intake (e.g. UdpTmFrameLink or
UdpTmDataLink), and command (uplink) packets are received as UDP datagrams from the YAMCS
UDP outlet (e.g. UdpTcFrameLink or UdpTcDataLink).
"""

import logging
import socket

LOGGER = logging.getLogger("yamcs_udp")

# Maximum size of a received UDP datagram
MAXIMUM_DATAGRAM_SIZE = 65507


class YamcsUdp:
    """UDP endpoints used to exchange packets with YAMCS

    Maintains two UDP sockets: a send socket used to push telemetry datagrams to the YAMCS
    UDP intake, and a receive socket bound locally to accept command datagrams from the
    YAMCS UDP outlet.
    """

    def __init__(self, tm_host, tm_port, tc_host, tc_port, timeout=0.500):
        """Initialize with YAMCS TM destination and local TC bind address

        Args:
            tm_host: hostname/address of the YAMCS UDP telemetry intake
            tm_port: port of the YAMCS UDP telemetry intake
            tc_host: local address to bind for receiving YAMCS command datagrams
            tc_port: local port to bind for receiving YAMCS command datagrams
            timeout: receive timeout in seconds allowing periodic shutdown checks
        """
        self.tm_destination = (tm_host, tm_port)
        self.tc_bind = (tc_host, tc_port)
        self.timeout = timeout
        self.tm_socket = None
        self.tc_socket = None

    def open(self):
        """Open the send and receive sockets"""
        self.tm_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tc_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tc_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tc_socket.bind(self.tc_bind)
        self.tc_socket.settimeout(self.timeout)
        LOGGER.info(
            "Sending TM to udp://%s:%d, receiving TC on udp://%s:%d",
            self.tm_destination[0],
            self.tm_destination[1],
            self.tc_bind[0],
            self.tc_bind[1],
        )

    def close(self):
        """Close both sockets"""
        for closing_socket in (self.tm_socket, self.tc_socket):
            if closing_socket is not None:
                try:
                    closing_socket.close()
                except OSError as error:
                    LOGGER.warning("Failed to close socket: %s", error)
        self.tm_socket = None
        self.tc_socket = None

    def send(self, packet):
        """Send one packet as a single datagram to the YAMCS telemetry intake

        Args:
            packet: bytes of the packet to send
        Returns:
            True when the datagram was sent, False otherwise
        """
        try:
            self.tm_socket.sendto(packet, self.tm_destination)
            return True
        except OSError as error:
            LOGGER.warning("Failed to send TM datagram: %s", error)
            return False

    def receive(self):
        """Receive one command datagram from the YAMCS outlet

        Blocks up to the configured timeout waiting for a datagram.

        Returns:
            datagram bytes, or b'' when no datagram arrived within the timeout
        """
        try:
            datagram, _ = self.tc_socket.recvfrom(MAXIMUM_DATAGRAM_SIZE)
            return datagram
        except socket.timeout:
            return b""
        except OSError as error:
            LOGGER.warning("Failed to receive TC datagram: %s", error)
            return b""
