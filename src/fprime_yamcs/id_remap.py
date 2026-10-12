""" Deterministic id remapping for telemetry channels at or above 0x80000000

YAMCS never matches an XTCE container restriction (e.g. `FPrimeChannelId == <id>`)
against a value of 0x80000000 or higher: it is evidently parsed/compared somewhere as
a signed 32-bit integer, so a restriction on e.g. 0xE0001000 (negative when read as
signed) never matches the same bytes decoded as an unsigned FwChanIdType. Confirmed
with a synthetic repro independent of F Prime: a from-scratch channel restricted on
such a value - scalar or aggregate - never gets a realtime value; the identical setup
restricted just below 0x80000000 works immediately, aggregate values included (YAMCS
publishes aggregates as a structured AggregateValue and supports dot-path access to
members, e.g. "Reading.pressure", for both the realtime value and archive history -
no flattening into separate scalar parameters is needed).

Some F Prime deployments assign subtopology-level components base ids at or above
0x80000000 (observed here: Bmp280 at 0xD0000000, MpuImu at 0xE0000000). This module
patches the generated XTCE's existing container restrictions for those channels, in
place, to use a remapped id instead - see xtce_remap.py.

Both the YAMCS launcher (patching the generated XTCE, see xtce_remap.py) and
fprime-yamcs-tlmchan (remapping re-injected packet bytes, see tlmchan/processor.py)
need to agree on the exact same remapped id for a given channel, but run as separate
processes with no shared state. compute_high_id_remap is a pure function of the
F Prime dictionary's contents so that both call sites, reading the same dictionary
file independently, always compute the identical mapping.

@author ivanlara

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
from pathlib import Path
from typing import Dict, Set

# YAMCS never matches a restriction/comparison on a value at or above this - see
# module docstring. Remapped ids are chosen below it.
SIGNED_INT_CEILING = 0x80000000


def _collect_used_ids(dictionary: dict) -> Set[int]:
    """Every id/opcode used anywhere in the dictionary (commands, channels, events,
    parameters, records) - the full universe a remapped id must avoid colliding with"""
    used: Set[int] = set()
    for command in dictionary.get("commands", []):
        if "opcode" in command:
            used.add(command["opcode"])
    for section in ("telemetryChannels", "events", "parameters", "records"):
        for item in dictionary.get(section, []):
            if "id" in item:
                used.add(item["id"])
    return used


def compute_high_id_remap(dictionary_path: Path) -> Dict[int, int]:
    """ Compute a deterministic id remapping for every telemetry channel in the given
    F Prime JSON dictionary whose id is at or above SIGNED_INT_CEILING.

    Args:
        dictionary_path: Path to the F Prime JSON topology dictionary
    Returns:
        Mapping from each such channel's real id to a remapped id below
        SIGNED_INT_CEILING, guaranteed not to collide with any id/opcode used anywhere
        else in the dictionary (nor with any other channel's remapped id)
    """
    with open(dictionary_path) as f:
        dictionary = json.load(f)

    reserved = _collect_used_ids(dictionary)

    high_channel_ids = sorted(
        channel["id"] for channel in dictionary.get("telemetryChannels", [])
        if channel.get("id", 0) >= SIGNED_INT_CEILING
    )

    remap: Dict[int, int] = {}
    candidate = SIGNED_INT_CEILING - 1
    for real_id in high_channel_ids:
        while candidate in reserved:
            candidate -= 1
        remap[real_id] = candidate
        reserved.add(candidate)
        candidate -= 1
    return remap
