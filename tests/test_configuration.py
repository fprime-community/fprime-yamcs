"""Regression checks for the shipped Yamcs file-transfer configuration."""

import re
from pathlib import Path

import pytest
import yaml

from fprime_yamcs import __main__ as main_module


INSTANCE_CONFIG = (
    Path(main_module.__file__).parent
    / "yamcs/src/main/yamcs/etc/yamcs.fprime-project.yaml"
)


@pytest.mark.parametrize("option, variable, default", [
    ("uplinkChunkSize", "FPRIME_UPLINK_CHUNK_SIZE", "512"),
    ("downlinkMirrorDir", "FPRIME_DOWNLINK_DIR", "/tmp/runner/fprime-downlink"),
])
def test_file_transfer_options_read_environment(option, variable, default):
    # Yamcs distinguishes env.NAME from Java/application properties named NAME.
    config = yaml.safe_load(INSTANCE_CONFIG.read_text(encoding="utf-8"))
    service, = (
        service for service in config["services"]
        if service["class"] == "com.example.myproject.FprimeFilePacketService"
    )
    assert service["args"][option] == "${env." + variable + ":" + default + "}"


def test_documented_uplink_chunk_fits_default_tc_frame():
    source = INSTANCE_CONFIG.read_text(encoding="utf-8")
    example = re.search(r"export FPRIME_UPLINK_CHUNK_SIZE=(\d+)", source)
    assert example is not None
    config = yaml.safe_load(source)
    tc_link, = (link for link in config["dataLinks"] if link["name"] == "UDP_TC_OUT")
    # Data packet: 13-byte F Prime header + 6-byte CCSDS space-packet header.
    # Unsegmented TC frame: 5-byte header + 2-byte CRC16.
    assert tc_link["errorDetection"] == "CRC16"
    assert int(example.group(1)) + 13 + 6 + 5 + 2 <= tc_link["maxFrameLength"]
