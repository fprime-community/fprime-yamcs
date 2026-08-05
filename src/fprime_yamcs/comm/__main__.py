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

LOGGER = logging.getLogger("fprime_yamcs_comm")


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
        print(
            "[ERROR] Comm adapter set to 'none'. Nothing to do but exit.",
            file=sys.stderr,
        )
        return 1

    adapter = Plugins.system().get_selected_class("communication")()
    framer = Plugins.system().get_selected_class("framing")()
    udp = YamcsUdp(args.tm_host, args.tm_port, args.tc_host, args.tc_port)
    LOGGER.info(
        "Bridging '%s' adapter and YAMCS UDP using '%s' framing",
        args.communication_selection,
        args.framing_selection,
    )

    bridge = UdpBridge(adapter, framer, udp)
    shutdown_event = threading.Event()

    def shutdown(*_):
        """Shutdown handler for signals"""
        shutdown_event.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    bridge.start()
    try:
        shutdown_event.wait()
    finally:
        bridge.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
