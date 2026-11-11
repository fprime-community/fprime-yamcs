"""Tests for the fprime-yamcs-tlmchan aggregate packet splitter

Unit tests cover splitting aggregate Svc.TlmChan packets into individual channel records
and re-injecting each record beyond the first as a standalone CCSDS space packet.
"""

import json
import socket
import struct

import pytest

from fprime_gds.common.models.serialize.numerical_types import U8Type, U32Type, U64Type
from fprime_gds.common.models.serialize.time_type import TimeType
from fprime_gds.common.templates.ch_template import ChTemplate

from fprime_yamcs.tlmchan.processor import FPrimeTlmChanProcessor


CHANNEL_TEMPLATES = {
    0x1001: ChTemplate(0x1001, "ChanU32", "Comp", U32Type),
    0x1002: ChTemplate(0x1002, "ChanU8", "Comp", U8Type),
    0x1003: ChTemplate(0x1003, "ChanU64", "Comp", U64Type),
}


def make_record(ch_id: int, value_bytes: bytes) -> bytes:
    """Serialize a single (id, time, value) channel record"""
    time_obj = TimeType()
    return struct.pack(">I", ch_id) + time_obj.serialize() + value_bytes


def make_packet(records: bytes, apid: int = FPrimeTlmChanProcessor.APID_TLM_CHAN) -> bytes:
    """Build a CCSDS space packet containing the given record payload"""
    payload = struct.pack(">H", 1) + records  # FwPacketDescriptorType + records
    header = struct.pack(">HHH", apid & 0x07FF, 0xC000, len(payload) - 1)
    return header + payload


class FakePacket:
    def __init__(self, binary):
        self.binary = binary


@pytest.fixture
def processor():
    """A processor with the YAMCS client bypassed and a loopback inject socket"""
    proc = FPrimeTlmChanProcessor.__new__(FPrimeTlmChanProcessor)
    proc.channel_dict = CHANNEL_TEMPLATES
    proc.sequence_count = 0
    proc.inject_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind(("127.0.0.1", 0))
    receiver.settimeout(2.0)
    proc.inject_address = receiver.getsockname()
    yield proc, receiver
    proc.inject_socket.close()
    receiver.close()


class TestSplitRecords:
    def test_single_record(self, processor):
        proc, _ = processor
        record = make_record(0x1001, struct.pack(">I", 42))
        assert proc._split_records(record) == [record]

    def test_multiple_records_variable_sizes(self, processor):
        proc, _ = processor
        records = [
            make_record(0x1001, struct.pack(">I", 42)),
            make_record(0x1002, struct.pack(">B", 7)),
            make_record(0x1003, struct.pack(">Q", 99)),
        ]
        assert proc._split_records(b"".join(records)) == records

    def test_unknown_channel_id(self, processor):
        proc, _ = processor
        record = make_record(0xDEAD, struct.pack(">I", 0))
        with pytest.raises(ValueError):
            proc._split_records(record)

    def test_truncated_record(self, processor):
        proc, _ = processor
        record = make_record(0x1001, struct.pack(">I", 42))
        with pytest.raises(Exception):
            proc._split_records(record[:-2])


class TestProcessChannelPacket:
    def receive_packets(self, receiver, count):
        packets = []
        for _ in range(count):
            packets.append(receiver.recv(2048))
        return packets

    def test_aggregate_packet_reinjects_later_records(self, processor):
        proc, receiver = processor
        records = [
            make_record(0x1001, struct.pack(">I", 1)),
            make_record(0x1002, struct.pack(">B", 2)),
            make_record(0x1003, struct.pack(">Q", 3)),
        ]
        proc._process_channel_packet(FakePacket(make_packet(b"".join(records))))
        injected = self.receive_packets(receiver, 2)
        for packet, record in zip(injected, records[1:]):
            apid = struct.unpack(">H", packet[0:2])[0] & 0x07FF
            length = struct.unpack(">H", packet[4:6])[0]
            assert apid == FPrimeTlmChanProcessor.APID_TLM_CHAN
            assert length == len(packet) - 6 - 1
            assert packet[6:8] == struct.pack(">H", 1)
            assert packet[8:] == record

    def test_single_record_packet_not_reinjected(self, processor):
        proc, receiver = processor
        record = make_record(0x1001, struct.pack(">I", 1))
        proc._process_channel_packet(FakePacket(make_packet(record)))
        with pytest.raises(socket.timeout):
            receiver.recv(2048)

    def test_non_channel_apid_ignored(self, processor):
        proc, receiver = processor
        records = make_record(0x1001, b"\x00" * 4) + make_record(0x1002, b"\x01")
        proc._process_channel_packet(FakePacket(make_packet(records, apid=2)))
        with pytest.raises(socket.timeout):
            receiver.recv(2048)

    def test_sequence_count_wraps(self, processor):
        proc, receiver = processor
        proc.sequence_count = FPrimeTlmChanProcessor.SEQUENCE_COUNT_MODULO - 1
        records = make_record(0x1001, struct.pack(">I", 1)) + make_record(
            0x1002, struct.pack(">B", 2)
        )
        proc._process_channel_packet(FakePacket(make_packet(records)))
        packet = receiver.recv(2048)
        assert struct.unpack(">H", packet[2:4])[0] == 0xC000


class FakeContainer:
    def __init__(self, qualified_name):
        self.qualified_name = qualified_name


class FakeMdb:
    def __init__(self, names):
        self.names = names

    def list_containers(self):
        return [FakeContainer(name) for name in self.names]


class FakeYamcsClient:
    def __init__(self, names):
        self.mdb = FakeMdb(names)

    def get_mdb(self, instance):
        return self.mdb


def container_processor(tmp_path, names, space_system=None, deployment="Ref.Ref"):
    """A processor with a fake MDB holding the given container names and a dictionary naming `deployment`"""
    dictionary = tmp_path / "dict.json"
    dictionary.write_text(json.dumps({"metadata": {"deploymentName": deployment}}))
    proc = FPrimeTlmChanProcessor.__new__(FPrimeTlmChanProcessor)
    proc.yamcs_client = FakeYamcsClient(names)
    proc.yamcs_instance = "fprime-project"
    proc.dictionary_path = dictionary
    proc.space_system = space_system
    return proc


class TestChannelContainerName:
    """The aggregate channel container is resolved under this deployment's space system"""

    CONTAINERS = ["/Ref_Ref/FPrimeTelemetryChannel", "/Ref_Ref/FPrimeEvent", "/Ground_Ground/FPrimeTelemetryChannel"]

    def test_explicit_space_system(self, tmp_path):
        proc = container_processor(tmp_path, self.CONTAINERS, space_system="Ground_Ground")
        assert proc._channel_container_name() == "/Ground_Ground/FPrimeTelemetryChannel"

    def test_space_system_derived_from_dictionary(self, tmp_path):
        proc = container_processor(tmp_path, self.CONTAINERS, deployment="Ground.Ground")
        assert proc._channel_container_name() == "/Ground_Ground/FPrimeTelemetryChannel"

    def test_single_candidate_under_another_name(self, tmp_path):
        proc = container_processor(tmp_path, ["/Other/FPrimeTelemetryChannel"], deployment="Ref.Ref")
        assert proc._channel_container_name() == "/Other/FPrimeTelemetryChannel"

    def test_explicit_space_system_missing_rejected(self, tmp_path):
        proc = container_processor(tmp_path, self.CONTAINERS, space_system="Nope")
        with pytest.raises(RuntimeError, match="/Nope/FPrimeTelemetryChannel"):
            proc._channel_container_name()

    def test_ambiguous_candidates_rejected(self, tmp_path):
        proc = container_processor(tmp_path, self.CONTAINERS, deployment="Other.Other")
        with pytest.raises(RuntimeError, match="--space-system"):
            proc._channel_container_name()

    def test_no_candidates_rejected(self, tmp_path):
        proc = container_processor(tmp_path, ["/Ref_Ref/FPrimeEvent"])
        with pytest.raises(RuntimeError, match="No 'FPrimeTelemetryChannel'"):
            proc._channel_container_name()
