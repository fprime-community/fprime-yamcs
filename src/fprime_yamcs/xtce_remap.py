""" XTCE post-processing: remap channel-id restrictions that YAMCS can never match

See id_remap.py for why: YAMCS never matches a container restriction against an id of
0x80000000 or higher, regardless of the channel's parameter type (scalar or
aggregate). This rewrites the "FPrimeChannelId == <id>" comparison on every affected
container's restriction criteria, in place, to the remapped id computed by
compute_high_id_remap - no new parameters or containers are added.

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

import xml.etree.ElementTree as ET
from pathlib import Path

from fprime_yamcs.id_remap import compute_high_id_remap

XTCE_NS = "http://www.omg.org/spec/XTCE/20180204"
_COMPARISON = f"{{{XTCE_NS}}}Comparison"

# The F Prime channel-id restriction parameter name every telemetry channel container
# is restricted on (see fprime-to-xtce) - only comparisons against this parameter are
# ever rewritten, so unrelated restrictions (e.g. a command's OpCode) are untouched.
CHANNEL_ID_PARAMETER_REF = "FPrimeChannelId"


def remap_high_channel_ids(xtce_path: Path, dictionary_path: Path) -> int:
    """ Rewrite every "FPrimeChannelId == <id>" restriction in the given XTCE file
    whose id is at or above SIGNED_INT_CEILING to its remapped equivalent, in place.

    Args:
        xtce_path: Path to the XTCE XML file to patch (modified in place)
        dictionary_path: Path to the F Prime JSON dictionary the XTCE was generated
            from, used to compute the id remap
    Returns:
        The number of channel-id restrictions remapped
    """
    id_remap = compute_high_id_remap(dictionary_path)
    if not id_remap:
        return 0

    ET.register_namespace("", XTCE_NS)
    tree = ET.parse(xtce_path)
    root = tree.getroot()

    remapped = 0
    for comparison in root.iter(_COMPARISON):
        if comparison.get("parameterRef") != CHANNEL_ID_PARAMETER_REF:
            continue
        real_id = int(comparison.get("value"))
        remapped_id = id_remap.get(real_id)
        if remapped_id is not None:
            comparison.set("value", str(remapped_id))
            remapped += 1

    tree.write(xtce_path, xml_declaration=True, encoding="utf-8")
    return remapped
