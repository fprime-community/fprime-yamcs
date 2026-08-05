""" Tests for the channel-id to serialized-value-size map generation """
import json

import pytest

from fprime_yamcs.__main__ import compute_channel_value_sizes, SIZE_STRING, SIZE_UNKNOWN


DICTIONARY = {
    "typeDefinitions": [
        {
            "kind": "enum",
            "qualifiedName": "Fw.On",
            "representationType": {"name": "I32", "kind": "integer", "size": 32, "signed": True},
        },
        {
            "kind": "alias",
            "qualifiedName": "FwSizeType",
            "underlyingType": {"name": "U64", "kind": "integer", "size": 64, "signed": False},
        },
        {
            "kind": "array",
            "qualifiedName": "Ref.Vector",
            "size": 3,
            "elementType": {"name": "F32", "kind": "float", "size": 32},
        },
        {
            "kind": "struct",
            "qualifiedName": "Ref.Frame",
            "members": {
                "header": {"type": {"name": "U32", "kind": "integer", "size": 32, "signed": False}, "index": 0},
                "pixels": {
                    "type": {"name": "U8", "kind": "integer", "size": 8, "signed": False},
                    "size": 16,
                    "index": 1,
                },
                "vector": {"type": {"name": "Ref.Vector", "kind": "qualifiedIdentifier"}, "index": 2},
                "state": {"type": {"name": "Fw.On", "kind": "qualifiedIdentifier"}, "index": 3},
            },
        },
        {
            "kind": "struct",
            "qualifiedName": "Ref.Variable",
            "members": {
                "name": {"type": {"name": "string", "kind": "string", "size": 40}, "index": 0},
                "value": {"type": {"name": "U32", "kind": "integer", "size": 32, "signed": False}, "index": 1},
            },
        },
        {
            "kind": "array",
            "qualifiedName": "Ref.Names",
            "size": 4,
            "elementType": {"name": "string", "kind": "string", "size": 10},
        },
    ],
    "telemetryChannels": [
        {"name": "Ref.u32Chan", "id": 100, "type": {"name": "U32", "kind": "integer", "size": 32, "signed": False}},
        {"name": "Ref.boolChan", "id": 101, "type": {"name": "bool", "kind": "bool", "size": 8}},
        {"name": "Ref.stringChan", "id": 102, "type": {"name": "string", "kind": "string", "size": 80}},
        {"name": "Ref.enumChan", "id": 103, "type": {"name": "Fw.On", "kind": "qualifiedIdentifier"}},
        {"name": "Ref.aliasChan", "id": 104, "type": {"name": "FwSizeType", "kind": "qualifiedIdentifier"}},
        {"name": "Ref.arrayChan", "id": 105, "type": {"name": "Ref.Vector", "kind": "qualifiedIdentifier"}},
        {"name": "Ref.structChan", "id": 106, "type": {"name": "Ref.Frame", "kind": "qualifiedIdentifier"}},
        {"name": "Ref.varStruct", "id": 107, "type": {"name": "Ref.Variable", "kind": "qualifiedIdentifier"}},
        {"name": "Ref.strArray", "id": 108, "type": {"name": "Ref.Names", "kind": "qualifiedIdentifier"}},
    ],
}


@pytest.fixture
def dictionary_file(tmp_path):
    path = tmp_path / "dictionary.json"
    path.write_text(json.dumps(DICTIONARY))
    return path


def test_compute_channel_value_sizes(dictionary_file):
    sizes = compute_channel_value_sizes(dictionary_file)
    assert sizes == {
        "100": 4,  # U32
        "101": 1,  # bool serializes as a single byte
        "102": SIZE_STRING,  # walkable via 2-byte length prefix
        "103": 4,  # enum with I32 representation
        "104": 8,  # alias of U64
        "105": 12,  # array of 3 F32
        "106": 4 + 16 + 12 + 4,  # struct: U32 + U8[16] + Vector + enum
        "107": SIZE_UNKNOWN,  # struct containing a string is variable-size
        "108": SIZE_UNKNOWN,  # array of strings is variable-size
    }


def test_empty_dictionary(tmp_path):
    path = tmp_path / "empty.json"
    path.write_text(json.dumps({"telemetryChannels": []}))
    assert compute_channel_value_sizes(path) == {}
