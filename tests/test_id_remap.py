"""Tests for id_remap: the deterministic id remap for channels YAMCS can't match"""

import json

from fprime_yamcs.id_remap import SIGNED_INT_CEILING, compute_high_id_remap

BASE_DICTIONARY = {
    "commands": [],
    "events": [],
    "parameters": [],
    "records": [],
    "telemetryChannels": [],
}


def write_dictionary(tmp_path, data):
    path = tmp_path / "dictionary.json"
    path.write_text(json.dumps(data))
    return path


def test_no_high_ids_yields_empty_remap(tmp_path):
    d = dict(BASE_DICTIONARY)
    d["telemetryChannels"] = [
        {"name": "comp.scalarChan", "type": {"name": "U32", "kind": "integer"}, "id": 5},
    ]
    path = write_dictionary(tmp_path, d)
    assert compute_high_id_remap(path) == {}


def test_high_id_channel_remapped_below_signed_int_ceiling(tmp_path):
    """Remapping is driven purely by id, not parameter type - a plain scalar channel at
    a high id is remapped exactly the same as an aggregate one would be."""
    d = dict(BASE_DICTIONARY)
    d["telemetryChannels"] = [
        {"name": "comp.scalarChan", "type": {"name": "U32", "kind": "integer"}, "id": 0xE0001000},
    ]
    path = write_dictionary(tmp_path, d)
    remap = compute_high_id_remap(path)
    assert remap == {0xE0001000: SIGNED_INT_CEILING - 1}


def test_remap_skips_values_already_used_anywhere_in_dictionary(tmp_path):
    """A channel's own id can coincidentally match a command opcode allocated from a
    separate counter in the same component; the remap must avoid every id/opcode in use."""
    d = dict(BASE_DICTIONARY)
    d["telemetryChannels"] = [
        {"name": "comp.highChan", "type": {"name": "U32", "kind": "integer"}, "id": 0xE0001000},
    ]
    d["commands"] = [{"name": "comp.SOME_PRM_SET", "opcode": SIGNED_INT_CEILING - 1}]
    path = write_dictionary(tmp_path, d)
    remap = compute_high_id_remap(path)
    assert remap == {0xE0001000: SIGNED_INT_CEILING - 2}  # top candidate reserved, falls back


def test_multiple_high_id_channels_get_distinct_remaps_in_sorted_order(tmp_path):
    d = dict(BASE_DICTIONARY)
    d["telemetryChannels"] = [
        {"name": "comp.bChan", "type": {"name": "U32", "kind": "integer"}, "id": 0xE0001001},
        {"name": "comp.aChan", "type": {"name": "U32", "kind": "integer"}, "id": 0xD0001000},
    ]
    path = write_dictionary(tmp_path, d)
    remap = compute_high_id_remap(path)
    assert remap == {
        0xD0001000: SIGNED_INT_CEILING - 1,
        0xE0001001: SIGNED_INT_CEILING - 2,
    }


def test_remap_is_deterministic_across_independent_calls(tmp_path):
    """Two unrelated processes (the launcher patching XTCE, tlmchan remapping packet
    bytes) must independently compute the identical mapping from the same file."""
    d = dict(BASE_DICTIONARY)
    d["telemetryChannels"] = [
        {"name": "comp.highChan", "type": {"name": "U32", "kind": "integer"}, "id": 0xE0001000},
    ]
    path = write_dictionary(tmp_path, d)
    assert compute_high_id_remap(path) == compute_high_id_remap(path)
