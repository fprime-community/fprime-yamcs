""" F Prime dictionary helpers shared by the launcher and the event/channel processors

@author LeStarch

Copyright 2026 LeStarch

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
from typing import Union


def deployment_name(dictionary: Union[str, Path]) -> str:
    """ Name of the XTCE root space system fprime-to-xtce generates for a dictionary

    fprime-to-xtce names the root SpaceSystem after `metadata.deploymentName` with dots replaced by underscores
    (e.g. "Ref.Ref" -> "Ref_Ref"), so every qualified parameter, command and container name in YAMCS starts with
    "/<deployment name>/".

    Args:
        dictionary: path to the F Prime JSON topology dictionary
    Returns:
        the root space system name
    """
    with open(str(dictionary)) as f:
        metadata = json.load(f).get("metadata", {})
    name = metadata.get("deploymentName")
    if not name:
        raise ValueError(f"metadata.deploymentName missing from F Prime dictionary {dictionary}")
    return name.replace(".", "_")
