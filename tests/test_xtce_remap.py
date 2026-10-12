"""Tests for xtce_remap: rewriting channel-id restrictions YAMCS can never match

YAMCS never matches an XTCE container restriction against an id of 0x80000000 or
higher, regardless of parameter type (scalar or aggregate) - see id_remap.py. These
tests cover rewriting the existing "FPrimeChannelId == <id>" restriction in place, for
any affected channel. No new parameters or containers are added: YAMCS natively
supports plotting/querying aggregate parameters (and their members, via dot-path, e.g.
"Reading.pressure") once the restriction itself can be matched.
"""

import shutil
import subprocess
import xml.etree.ElementTree as ET
import json

import pytest

from fprime_yamcs.id_remap import SIGNED_INT_CEILING
from fprime_yamcs.xtce_remap import XTCE_NS, remap_high_channel_ids

NS = {"x": XTCE_NS}


def write_xtce(tmp_path, body: str):
    """Write a minimal XTCE document (root SpaceSystem + the given inner XML) to a file"""
    xtce_path = tmp_path / "fprime.xtce.xml"
    xtce_path.write_text(f"""<?xml version='1.0' encoding='utf-8'?>
<SpaceSystem xmlns="{XTCE_NS}" name="Root">
{body}
</SpaceSystem>
""")
    return xtce_path


def write_dictionary(tmp_path, data, name="dictionary.json"):
    """Write an F Prime JSON dictionary (or fragment) to a file, for compute_high_id_remap"""
    dictionary_path = tmp_path / name
    dictionary_path.write_text(json.dumps(data))
    return dictionary_path


def channel_doc(channel_id: int) -> str:
    return f"""
  <TelemetryMetaData>
    <ParameterTypeSet>
      <IntegerParameterType name="U32" sizeInBits="32" signed="false">
        <IntegerDataEncoding sizeInBits="32" encoding="unsigned" />
      </IntegerParameterType>
    </ParameterTypeSet>
  </TelemetryMetaData>
  <SpaceSystem name="comp">
    <TelemetryMetaData>
      <ParameterSet>
        <Parameter name="Count" parameterTypeRef="/Root/U32" />
      </ParameterSet>
      <ContainerSet>
        <SequenceContainer name="Count">
          <EntryList>
            <ParameterRefEntry parameterRef="comp/Count" />
          </EntryList>
          <BaseContainer containerRef="FPrimeTelemetryChannel">
            <RestrictionCriteria>
              <ComparisonList>
                <Comparison parameterRef="FPrimeChannelId" value="{channel_id}" />
              </ComparisonList>
            </RestrictionCriteria>
          </BaseContainer>
        </SequenceContainer>
      </ContainerSet>
    </TelemetryMetaData>
  </SpaceSystem>
"""


def test_high_id_restriction_rewritten(tmp_path):
    xtce_path = write_xtce(tmp_path, channel_doc(0xE0001000))
    dictionary_path = write_dictionary(tmp_path, {
        "commands": [], "events": [], "parameters": [], "records": [],
        "telemetryChannels": [{"name": "comp.Count", "type": {"name": "U32", "kind": "integer"}, "id": 0xE0001000}],
    })

    count = remap_high_channel_ids(xtce_path, dictionary_path)
    assert count == 1

    root = ET.parse(xtce_path).getroot()
    comparison = root.find(".//x:Comparison", NS)
    assert comparison.get("value") == str(SIGNED_INT_CEILING - 1)


def test_low_id_restriction_untouched(tmp_path):
    xtce_path = write_xtce(tmp_path, channel_doc(300))
    dictionary_path = write_dictionary(tmp_path, {
        "commands": [], "events": [], "parameters": [], "records": [],
        "telemetryChannels": [{"name": "comp.Count", "type": {"name": "U32", "kind": "integer"}, "id": 300}],
    })

    count = remap_high_channel_ids(xtce_path, dictionary_path)
    assert count == 0

    root = ET.parse(xtce_path).getroot()
    comparison = root.find(".//x:Comparison", NS)
    assert comparison.get("value") == "300"


def test_only_channel_id_comparisons_are_touched(tmp_path):
    """A Comparison on some other parameter (e.g. a command's OpCode) that happens to
    carry a high value must never be rewritten - only FPrimeChannelId restrictions are."""
    body = """
  <SpaceSystem name="comp">
    <CommandMetaData>
      <MetaCommandSet>
        <MetaCommand name="SOME_CMD">
          <ArgumentAssignmentList>
            <ArgumentAssignment argumentName="OpCode" argumentValue="3758100480" />
          </ArgumentAssignmentList>
        </MetaCommand>
      </MetaCommandSet>
    </CommandMetaData>
  </SpaceSystem>
"""
    xtce_path = write_xtce(tmp_path, body)
    dictionary_path = write_dictionary(tmp_path, {
        "commands": [{"name": "comp.SOME_CMD", "opcode": 3758100480}],
        "events": [], "parameters": [], "records": [], "telemetryChannels": [],
    })

    count = remap_high_channel_ids(xtce_path, dictionary_path)
    assert count == 0

    root = ET.parse(xtce_path).getroot()
    assignment = root.find(".//x:ArgumentAssignment", NS)
    assert assignment.get("argumentValue") == "3758100480"


MINI_FPRIME_DICTIONARY = {
    "metadata": {
        "deploymentName": "Mini.Mini",
        "projectVersion": "test",
        "frameworkVersion": "test",
        "dictionarySpecVersion": "1.0.0",
    },
    "typeDefinitions": [
        {
            "kind": "struct",
            "qualifiedName": "Flat.FlatData",
            "members": {
                "pressure": {"type": {"name": "F32", "kind": "float", "size": 32}, "index": 0},
                "temperature": {"type": {"name": "F32", "kind": "float", "size": 32}, "index": 1},
            },
            "default": {"pressure": 0.0, "temperature": 0.0},
        },
    ],
    "constants": [],
    "commands": [],
    "parameters": [],
    "events": [],
    "telemetryChannels": [
        {"name": "comp.aggChan", "type": {"name": "Flat.FlatData", "kind": "qualifiedIdentifier"},
         "id": 0xE0001000, "telemetryUpdate": "always"},
        {"name": "comp.scalarChan", "type": {"name": "U32", "kind": "integer", "size": 32, "signed": False},
         "id": 300, "telemetryUpdate": "always"},
    ],
    "records": [],
    "containers": [],
    "telemetryPacketSets": [],
}


@pytest.mark.skipif(shutil.which("fprime-to-xtce") is None, reason="fprime-to-xtce not installed")
def test_remap_against_real_fprime_to_xtce_output(tmp_path):
    """Regression safety net: run the real fprime-to-xtce converter against a minimal F
    Prime dictionary with a high-id aggregate channel and a low-id scalar one, then
    remap, and confirm only the aggregate channel's restriction changed - without
    adding any new parameter or container."""
    dictionary_path = write_dictionary(tmp_path, MINI_FPRIME_DICTIONARY)
    xtce_path = tmp_path / "fprime.xtce.xml"

    subprocess.run(["fprime-to-xtce", "-o", str(xtce_path), str(dictionary_path)], check=True)
    count = remap_high_channel_ids(xtce_path, dictionary_path)
    assert count == 1

    root = ET.parse(xtce_path).getroot()
    comp = root.find(".//x:SpaceSystem[@name='comp']", NS)

    # No new Parameter/SequenceContainer added - still exactly the two originals
    params = {p.get("name") for p in comp.findall("x:TelemetryMetaData/x:ParameterSet/x:Parameter", NS)}
    containers = {c.get("name") for c in comp.findall("x:TelemetryMetaData/x:ContainerSet/x:SequenceContainer", NS)}
    assert params == {"aggChan", "scalarChan"}
    assert containers == {"aggChan", "scalarChan"}

    agg_container = comp.find("x:TelemetryMetaData/x:ContainerSet/x:SequenceContainer[@name='aggChan']", NS)
    agg_comparison = agg_container.find("x:BaseContainer/x:RestrictionCriteria/x:ComparisonList/x:Comparison", NS)
    assert agg_comparison.get("value") == str(SIGNED_INT_CEILING - 1)

    scalar_container = comp.find("x:TelemetryMetaData/x:ContainerSet/x:SequenceContainer[@name='scalarChan']", NS)
    scalar_comparison = scalar_container.find(
        "x:BaseContainer/x:RestrictionCriteria/x:ComparisonList/x:Comparison", NS)
    assert scalar_comparison.get("value") == "300"
