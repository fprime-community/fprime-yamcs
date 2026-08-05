"""Tests for the fprime-yamcs-comm bridge

Unit tests cover the packaged no-op framer/deframer. Integration tests flow data through
the full bridge: a socat-provided PTY pair stands in for a UART endpoint on one side, and
UDP sockets stand in for the YAMCS intake/outlet on the other.
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from fprime_gds.common.communication.framing import FpFramerDeframer
from fprime_yamcs.comm.framing import NoOpFramerDeframer

SOCAT = shutil.which("socat")
TIMEOUT = 10.0


class TestNoOpFramerDeframer:
    """Unit tests for the no-op framer/deframer"""

    def test_frame_passthrough(self):
        framer = NoOpFramerDeframer()
        assert framer.frame(b"hello") == b"hello"
        assert framer.frame(b"") == b""

    def test_deframe_passthrough(self):
        framer = NoOpFramerDeframer()
        packet, leftover, discarded = framer.deframe(b"hello")
        assert packet == b"hello"
        assert leftover == b""
        assert discarded == b""

    def test_deframe_empty(self):
        framer = NoOpFramerDeframer()
        packet, leftover, discarded = framer.deframe(b"")
        assert packet is None
        assert leftover == b""
        assert discarded == b""

    def test_deframe_all(self):
        framer = NoOpFramerDeframer()
        packets, leftover, discarded = framer.deframe_all(b"hello", no_copy=False)
        assert packets == [b"hello"]
        assert leftover == b""
        assert discarded == b""


def read_available(fd, minimum=1, timeout=TIMEOUT):
    """Read at least `minimum` bytes from a non-blocking fd within timeout"""
    end = time.time() + timeout
    data = b""
    while time.time() < end and len(data) < minimum:
        try:
            data += os.read(fd, 4096)
        except BlockingIOError:
            time.sleep(0.05)
    return data


@pytest.fixture
def pty_pair(tmp_path):
    """Create a linked PTY pair using socat"""
    link_a = tmp_path / "ttyA"
    link_b = tmp_path / "ttyB"
    process = subprocess.Popen(
        [
            SOCAT,
            f"pty,raw,echo=0,link={link_a}",
            f"pty,raw,echo=0,link={link_b}",
        ]
    )
    end = time.time() + TIMEOUT
    while time.time() < end and not (link_a.exists() and link_b.exists()):
        time.sleep(0.05)
    assert link_a.exists() and link_b.exists(), "socat failed to create PTY pair"
    yield link_a, link_b
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=TIMEOUT)


def start_bridge(uart_device: Path, tm_port: int, tc_port: int, framing: str):
    """Start the fprime-yamcs-comm bridge process"""
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "fprime_yamcs.comm",
            "--communication-selection",
            "uart",
            "--uart-device",
            str(uart_device),
            "--uart-skip-port-check",
            "--framing-selection",
            framing,
            "--tm-port",
            str(tm_port),
            "--tc-port",
            str(tc_port),
        ]
    )


@pytest.fixture(params=["no-op", "fprime"])
def bridge_setup(request, pty_pair, unused_udp_ports):
    """Run the bridge against one PTY end, exposing the peer PTY and UDP sockets"""
    link_a, link_b = pty_pair
    tm_port, tc_port = unused_udp_ports

    tm_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tm_socket.bind(("127.0.0.1", tm_port))
    tm_socket.settimeout(TIMEOUT)
    tc_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    bridge = start_bridge(link_a, tm_port, tc_port, request.param)
    peer_fd = os.open(link_b, os.O_RDWR | os.O_NONBLOCK)
    # Allow the bridge to open its resources
    time.sleep(2.0)
    assert bridge.poll() is None, "Bridge process exited prematurely"
    yield request.param, peer_fd, tm_socket, tc_socket, tc_port
    bridge.send_signal(signal.SIGINT)
    bridge.wait(timeout=TIMEOUT)
    os.close(peer_fd)
    tm_socket.close()
    tc_socket.close()


@pytest.fixture
def unused_udp_ports():
    """Reserve two distinct UDP port numbers"""
    sockets = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM) for _ in range(2)]
    for reservation in sockets:
        reservation.bind(("127.0.0.1", 0))
    ports = [reservation.getsockname()[1] for reservation in sockets]
    for reservation in sockets:
        reservation.close()
    return ports


@pytest.mark.skipif(SOCAT is None, reason="socat is not available")
class TestBridgeFlow:
    """Integration tests flowing data through the bridge in both directions"""

    def test_uart_to_udp(self, bridge_setup):
        """Endpoint -> bridge -> YAMCS UDP intake"""
        framing, peer_fd, tm_socket, _, _ = bridge_setup
        payload = b"telemetry-packet-payload"
        wire_data = (
            payload if framing == "no-op" else FpFramerDeframer().frame(payload)
        )
        os.write(peer_fd, wire_data)
        datagram, _ = tm_socket.recvfrom(65507)
        assert datagram == payload

    def test_udp_to_uart(self, bridge_setup):
        """YAMCS UDP outlet -> bridge -> endpoint"""
        framing, peer_fd, _, tc_socket, tc_port = bridge_setup
        payload = b"command-packet-payload"
        tc_socket.sendto(payload, ("127.0.0.1", tc_port))
        expected = (
            payload if framing == "no-op" else FpFramerDeframer().frame(payload)
        )
        received = read_available(peer_fd, minimum=len(expected))
        assert received == expected
