"""fprime_yamcs.comm.__main__: entry point for the fprime-yamcs-comm bridge

fprime-yamcs-comm bridges an F Prime endpoint (reached through an F Prime GDS
communication adapter plugin: UART, IP, etc.) and the YAMCS UDP intake/outlet. A single
stage of framing/deframing (an F Prime GDS framing plugin) sits between the two sides.

The default framing plugin is the packaged "no-op" framer/deframer, which passes data
through unchanged since YAMCS nominally performs framing/deframing itself. Select
`--framing-selection fprime` (or any other installed framing plugin) to have the bridge
perform the framing/deframing stage instead.
"""

import logging
import signal
import sys
import threading
from typing import Any, Dict, Tuple

# Required adapters built on standard tools
import fprime_gds.common.communication.adapters.base
import fprime_gds.common.communication.adapters.ip
import fprime_gds.executables.cli
from fprime_gds.plugin.system import Plugins

from fprime_yamcs.comm.bridge import UdpBridge
from fprime_yamcs.comm.udp import YamcsUdp

# Uses non-standard PIP package pyserial, so test the waters before getting a hard-import crash
try:
    import fprime_gds.common.communication.adapters.uart
except ImportError:
    pass

LOGGER = logging.getLogger(__name__)

# Adapters known to expose a byte stream, where read-chunk boundaries are arbitrary and
# no-op framing cannot reliably preserve packet boundaries
STREAM_ADAPTERS = {"uart", "ip"}


class YamcsUdpParser(fprime_gds.executables.cli.ParserBase):
    """Parser for the YAMCS UDP intake/outlet options"""

    DESCRIPTION = "YAMCS UDP Options"

    def get_arguments(self) -> Dict[Tuple[str, ...], Dict[str, Any]]:
        """Arguments for the YAMCS UDP side of the bridge"""
        return {
            ("--tm-host",): {
                "dest": "tm_host",
                "type": str,
                "default": "127.0.0.1",
                "help": "Host of the YAMCS UDP telemetry intake to send packets to.",
            },
            ("--tm-port",): {
                "dest": "tm_port",
                "type": int,
                "default": 50000,
                "help": "Port of the YAMCS UDP telemetry intake to send packets to.",
            },
            ("--tc-host",): {
                "dest": "tc_host",
                "type": str,
                "default": "127.0.0.1",
                "help": "Local address to bind for receiving YAMCS UDP command packets.",
            },
            ("--tc-port",): {
                "dest": "tc_port",
                "type": int,
                "default": 50001,
                "help": "Local port to bind for receiving YAMCS UDP command packets.",
            },
            ("--tc-allowed-source",): {
                "dest": "tc_sources",
                "type": str,
                "action": "append",
                "default": None,
                "help": "Additional source address allowed to send command packets "
                "(repeatable). The TM host and loopback are always allowed.",
            },
        }

    def handle_arguments(self, args, **kwargs):
        """Validate the YAMCS UDP arguments"""
        for port in (args.tm_port, args.tc_port):
            if not 0 < port <= 65535:
                raise ValueError(f"Invalid UDP port: {port}")
        return args


class YamcsPluginArgumentParser(fprime_gds.executables.cli.PluginArgumentParser):
    """Plugin parser defaulting framing to the packaged no-op implementation"""

    FPRIME_CHOICES = {
        **fprime_gds.executables.cli.PluginArgumentParser.FPRIME_CHOICES,
        "framing": "no-op",
    }


def main():
    """Run the fprime-yamcs-comm bridge"""
    logging.basicConfig(level=logging.INFO)
    # fprime-yamcs-comm supports 2 and only 2 plugin categories
    Plugins.system(["communication", "framing"])
    args, _ = fprime_gds.executables.cli.ParserBase.parse_args(
        [YamcsUdpParser, YamcsPluginArgumentParser],
        description="F Prime to YAMCS UDP communication bridge.",
    )
    if args.communication_selection == "none":
        LOGGER.error("Comm adapter set to 'none'. Nothing to do but exit.")
        return 1
    if (
        args.framing_selection == "no-op"
        and args.communication_selection in STREAM_ADAPTERS
    ):
        LOGGER.warning(
            "'no-op' framing over the stream-oriented '%s' adapter cannot preserve "
            "packet boundaries: packets may be split or merged across UDP datagrams "
            "depending on read timing. Use a boundary-recovering framing plugin "
            "(e.g. --framing-selection fprime) unless the endpoint stream carries "
            "self-delimiting data that YAMCS deframes.",
            args.communication_selection,
        )

    adapter = Plugins.system().get_selected_class("communication")()
    framer = Plugins.system().get_selected_class("framing")()
    udp = YamcsUdp(
        args.tm_host, args.tm_port, args.tc_host, args.tc_port, args.tc_sources
    )
    LOGGER.info(
        "Bridging '%s' adapter and YAMCS UDP using '%s' framing",
        args.communication_selection,
        args.framing_selection,
    )

    shutdown_event = threading.Event()
    failure_event = threading.Event()

    def fail(*_):
        """Failure handler for abnormal pump-thread exits"""
        failure_event.set()
        shutdown_event.set()

    bridge = UdpBridge(adapter, framer, udp, failure_handler=fail)

    def shutdown(*_):
        """Shutdown handler for signals"""
        shutdown_event.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        bridge.start()
    except OSError as error:
        LOGGER.error("Failed to open bridge resources: %s", error)
        return 1
    try:
        shutdown_event.wait()
    finally:
        bridge.stop()
    return 1 if failure_event.is_set() else 0


if __name__ == "__main__":
    sys.exit(main())
