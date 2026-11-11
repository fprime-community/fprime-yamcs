""" Tests for fprime_yamcs.events.processor: event container resolution

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

import pytest

from fprime_yamcs.events.processor import FPrimeEventProcessor
from test_tlmchan import FakeYamcsClient


def event_processor(tmp_path, names, space_system=None, deployment="Ref.Ref"):
    """An event processor with a fake MDB holding the given container names"""
    dictionary = tmp_path / "dict.json"
    dictionary.write_text(json.dumps({"metadata": {"deploymentName": deployment}}))
    proc = FPrimeEventProcessor.__new__(FPrimeEventProcessor)
    proc.yamcs_client = FakeYamcsClient(names)
    proc.yamcs_instance = "fprime-project"
    proc.dictionary_path = dictionary
    proc.space_system = space_system
    return proc


class TestEventContainerName:
    CONTAINERS = ["/Ref_Ref/FPrimeEvent", "/Ground_Ground/FPrimeEvent", "/Ground_Ground/FPrimeTelemetryChannel"]

    def test_explicit_space_system(self, tmp_path):
        assert event_processor(tmp_path, self.CONTAINERS, "Ground_Ground")._event_container_name() == \
            "/Ground_Ground/FPrimeEvent"

    def test_derived_from_dictionary(self, tmp_path):
        assert event_processor(tmp_path, self.CONTAINERS)._event_container_name() == "/Ref_Ref/FPrimeEvent"

    def test_ambiguous_without_match_rejected(self, tmp_path):
        with pytest.raises(RuntimeError, match="--space-system"):
            event_processor(tmp_path, self.CONTAINERS, deployment="Other.Other")._event_container_name()
