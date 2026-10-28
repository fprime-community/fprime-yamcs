"""Tests that the processors decode with the types defined in the topology dictionary

F Prime serializes string lengths with FwSizeStoreType, whose width is configured per project and
published in the dictionary's typeDefinitions. The decoders must use that width rather than the
fprime-gds default (U16), or every string argument is mis-parsed.
"""

import json
import struct

import pytest

from fprime_gds.common.models.serialize.numerical_types import U16Type
from fprime_gds.common.models.serialize.time_type import TimeType
from fprime_gds.common.utils.config_manager import ConfigManager

from fprime_yamcs.events.processor import FPrimeEventProcessor
from fprime_yamcs.tlmchan.processor import FPrimeTlmChanProcessor


EVENT_ID = 0x1000
OPCODE_EVENT_ID = 0x1001
COUNT_EVENT_ID = 0x1002
CHANNEL_ID = 0x2000
OPCODE = 0x500
MESSAGE = "test string 0.123456789 abcdefghijk"


def integer_type(name: str, bits: int) -> dict:
    return {"name": name, "kind": "integer", "size": bits, "signed": False}


def write_dictionary(tmp_path, size_store_bits: int):
    """Write a minimal topology dictionary whose FwSizeStoreType has the given width"""
    string_type = {"name": "string", "kind": "string", "size": 40}
    dictionary = {
        "metadata": {
            "deploymentName": "Test",
            "projectVersion": "0.0.0",
            "frameworkVersion": "0.0.0",
            "libraryVersions": [],
            "dictionarySpecVersion": "1.0.0",
        },
        "typeDefinitions": [
            {
                "kind": "alias",
                "qualifiedName": "FwSizeStoreType",
                "type": integer_type(f"U{size_store_bits}", size_store_bits),
                "underlyingType": integer_type(f"U{size_store_bits}", size_store_bits),
            },
            {
                "kind": "alias",
                "qualifiedName": "FwEventIdType",
                "type": integer_type("U32", 32),
                "underlyingType": integer_type("U32", 32),
            },
            {
                "kind": "alias",
                "qualifiedName": "FwChanIdType",
                "type": integer_type("U32", 32),
                "underlyingType": integer_type("U32", 32),
            },
            {
                "kind": "alias",
                "qualifiedName": "FwOpcodeType",
                "type": integer_type("U32", 32),
                "underlyingType": integer_type("U32", 32),
            },
        ],
        "constants": [],
        "commands": [{"name": "Comp.DO_THING", "commandKind": "sync", "opcode": OPCODE, "formalParams": []}],
        "parameters": [],
        "events": [
            {
                "name": "Comp.StringEvent",
                "severity": "ACTIVITY_HI",
                "formalParams": [{"name": "message", "type": string_type, "ref": False}],
                "id": EVENT_ID,
                "format": "message={}",
            },
            {
                "name": "Comp.OpCodeDispatched",
                "severity": "COMMAND",
                "formalParams": [
                    {"name": "Opcode", "type": {"name": "FwOpcodeType", "kind": "qualifiedIdentifier"}, "ref": False},
                    {"name": "port", "type": integer_type("U32", 32), "ref": False},
                ],
                "id": OPCODE_EVENT_ID,
                "format": "Opcode 0x{x} dispatched to port {}.",
            },
            {
                "name": "Comp.Counted",
                "severity": "ACTIVITY_LO",
                "formalParams": [{"name": "opcode", "type": integer_type("U32", 32), "ref": False}],
                "id": COUNT_EVENT_ID,
                "format": "Counted {}",
            },
        ],
        "telemetryChannels": [
            {
                "name": "Comp.StringChannel",
                "type": string_type,
                "id": CHANNEL_ID,
                "telemetryUpdate": "always",
            }
        ],
        "records": [],
        "containers": [],
        "telemetryPacketSets": [],
    }
    path = tmp_path / "TestTopologyDictionary.json"
    path.write_text(json.dumps(dictionary))
    return path


def serialize_string(value: str, size_store_bits: int) -> bytes:
    """Serialize a string as F Prime does: FwSizeStoreType length prefix followed by the characters"""
    encoded = value.encode("utf-8")
    return struct.pack(">" + {16: "H", 32: "I", 64: "Q"}[size_store_bits], len(encoded)) + encoded


def make_packet(apid: int, body: bytes) -> bytes:
    """Build a CCSDS space packet with the F Prime packet descriptor followed by body"""
    payload = struct.pack(">H", apid) + body
    header = struct.pack(">HHH", apid & 0x07FF, 0xC000, len(payload) - 1)
    return header + payload


class FakePacket:
    def __init__(self, binary):
        self.binary = binary
        self.generation_time = None


@pytest.fixture(autouse=True)
def reset_size_store_type():
    """Restore the fprime-gds default so each test starts from a fresh configuration"""
    yield
    ConfigManager().set_type("FwSizeStoreType", U16Type)


@pytest.mark.parametrize("size_store_bits", [16, 32, 64])
def test_event_string_decoded_with_dictionary_size_type(tmp_path, size_store_bits):
    processor = FPrimeEventProcessor.__new__(FPrimeEventProcessor)
    processor.dictionary_path = write_dictionary(tmp_path, size_store_bits)
    processor._init_fprime_decoder()

    assert ConfigManager().get_type("FwSizeStoreType").getSize() == size_store_bits // 8

    body = struct.pack(">I", EVENT_ID) + TimeType().serialize() + serialize_string(MESSAGE, size_store_bits)
    event_data = processor._extract_event_data(make_packet(FPrimeEventProcessor.APID_EVENT, body))
    events = processor.event_decoder.decode_api(event_data)

    assert len(events) == 1
    assert events[0].get_args()[0].val == MESSAGE


@pytest.mark.parametrize(
    "event_id, args, message",
    [
        (OPCODE_EVENT_ID, (OPCODE, 2), "Opcode Comp.DO_THING dispatched to port 2."),
        (OPCODE_EVENT_ID, (0x999, 2), "Opcode 0x999 dispatched to port 2."),
        (COUNT_EVENT_ID, (OPCODE,), f"Counted {OPCODE}"),
    ],
)
def test_event_message_names_opcode_arguments(tmp_path, event_id, args, message):
    processor = FPrimeEventProcessor.__new__(FPrimeEventProcessor)
    processor.dictionary_path = write_dictionary(tmp_path, 16)
    processor._init_fprime_decoder()

    body = struct.pack(">I", event_id) + TimeType().serialize() + struct.pack(f">{len(args)}I", *args)
    event_data = processor._extract_event_data(make_packet(FPrimeEventProcessor.APID_EVENT, body))
    events = processor.event_decoder.decode_api(event_data)

    assert processor._display_text(events[0]) == message


@pytest.mark.parametrize("size_store_bits", [16, 32, 64])
def test_channel_string_decoded_with_dictionary_size_type(tmp_path, size_store_bits):
    processor = FPrimeTlmChanProcessor.__new__(FPrimeTlmChanProcessor)
    processor.dictionary_path = write_dictionary(tmp_path, size_store_bits)
    processor._init_fprime_decoder()

    record = struct.pack(">I", CHANNEL_ID) + TimeType().serialize() + serialize_string(MESSAGE, size_store_bits)
    channels = processor.channel_decoder.decode_api(record)

    assert len(channels) == 1
    assert channels[0].get_val() == MESSAGE
