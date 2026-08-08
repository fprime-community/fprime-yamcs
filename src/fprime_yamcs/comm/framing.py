"""fprime_yamcs.comm.framing: no-op framer/deframer plugin

Provides a pass-through FramerDeframer implementation for use when YAMCS performs the
framing/deframing itself. Framing returns the data unchanged, and deframing treats each
chunk of received bytes as a single packet.
"""

from fprime_gds.common.communication.framing import FramerDeframer
from fprime_gds.plugin.definitions import gds_plugin_implementation


class NoOpFramerDeframer(FramerDeframer):
    """Pass-through framer/deframer

    Data is passed through unmodified in both directions. Each buffer of bytes handed to
    `deframe` is treated as exactly one packet. This is the correct behavior when the peer
    system (e.g. YAMCS) applies and removes any framing itself.
    """

    def frame(self, data):
        """Return the data unchanged"""
        return data

    def deframe(self, data, no_copy=False):
        """Treat the entire available buffer as a single packet"""
        if data:
            return data, b"", b""
        return None, data, b""

    @classmethod
    def get_name(cls):
        """Get the name of this plugin"""
        return "no-op"

    @classmethod
    @gds_plugin_implementation
    def register_framing_plugin(cls):
        """Register the no-op framer/deframer plugin"""
        return cls
