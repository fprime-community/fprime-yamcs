""" Tests for fprime_yamcs.__main__: launcher argument handling and configuration rewriting

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
import sys
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import pytest

from fprime_gds.executables.cli import ParserBase
from fprime_gds.plugin.system import Plugins

from fprime_yamcs import __main__ as main_module
from fprime_yamcs.__main__ import (
    LAUNCHER_PLUGIN_CATEGORIES,
    SECONDARY_PORT_STRIDE,
    TC_FRAME_LINK,
    TM_FRAME_LINK,
    TM_PACKET_LINK,
    XTCE_ROOT_CONTAINER,
    YamcsParser,
    YamcsPluginArgumentParser,
    anchor_relative_mdb_paths,
    check_comm_bridge_ports,
    check_communication_selection,
    comm_bridge_arguments,
    configure_secondary_spacecraft,
    get_dictionary_constant,
    launch_comm_bridge,
    launch_deployment_app,
    launch_secondary_comm_bridges,
    launch_yamcs_maven,
    needs_comm_bridge,
    secondary_comm_bridge_arguments,
    secondary_ports,
    without_options,
)
from fprime_yamcs.dictionary import deployment_name

DICTIONARY = Path("/deploy/dict/RefTopologyDictionary.json")


def make_args(tmp_path, **overrides):
    """Build a parsed-argument namespace with valid defaults"""
    values = {
        "yamcs_config_dir": tmp_path,
        "yamcs_web_extension_dirs": [],
        "yamcs_plugin_jars": [],
        "yamcs_sdls_key_file": None,
        "yamcs_sdls_spi": 1,
        "secondary_dictionary": [],
        "secondary_bridge_arguments": [],
    }
    values.update(overrides)
    return Namespace(**values)


def write_dictionary(path, deployment, spacecraft_id, frame_size=1024):
    """Write a minimal F Prime dictionary carrying the constants the launcher reads"""
    path.write_text(json.dumps({
        "metadata": {"deploymentName": deployment},
        "constants": [
            {"qualifiedName": "ComCfg.SpacecraftId", "value": spacecraft_id},
            {"qualifiedName": "ComCfg.TmFrameFixedSize", "value": frame_size},
        ],
    }))
    return path


class TestHandleArguments:
    """YamcsParser.handle_arguments validation of plugin jars and web extension dirs"""

    def handle(self, args, discovered=()):
        with patch.object(main_module, "discovered_web_extension_dirs",
                          return_value=list(discovered)):
            return YamcsParser().handle_arguments(args)

    def test_missing_plugin_jar_rejected(self, tmp_path):
        args = make_args(tmp_path, yamcs_plugin_jars=[tmp_path / "missing.jar"])
        with pytest.raises(Exception, match="does not exist"):
            self.handle(args)

    def test_empty_plugin_jar_directory_warns(self, tmp_path, capsys):
        empty = tmp_path / "empty"
        empty.mkdir()
        args = self.handle(make_args(tmp_path, yamcs_plugin_jars=[empty]))
        assert args.yamcs_plugin_jars == [empty]
        assert "contains no *.jar files" in capsys.readouterr().err

    def test_explicit_extension_dir_must_exist(self, tmp_path):
        args = make_args(tmp_path, yamcs_web_extension_dirs=[tmp_path / "missing"])
        with pytest.raises(Exception, match="is not a directory"):
            self.handle(args)

    def test_explicit_extension_dir_rejects_whitespace(self, tmp_path):
        spaced = tmp_path / "has space"
        spaced.mkdir()
        args = make_args(tmp_path, yamcs_web_extension_dirs=[spaced])
        with pytest.raises(Exception, match="commas or whitespace"):
            self.handle(args)

    def test_discovered_extension_dirs_appended(self, tmp_path):
        discovered = tmp_path / "discovered"
        discovered.mkdir()
        args = self.handle(make_args(tmp_path), discovered=[discovered])
        assert args.yamcs_web_extension_dirs == [discovered]

    def test_bad_discovered_extension_dir_skipped(self, tmp_path, capsys):
        good = tmp_path / "good"
        good.mkdir()
        bad = tmp_path / "has space"
        bad.mkdir()
        args = self.handle(make_args(tmp_path), discovered=[bad, good])
        assert args.yamcs_web_extension_dirs == [good]
        assert "Skipping discovered web extension" in capsys.readouterr().err


    def test_secondary_dictionary_must_be_file(self, tmp_path):
        args = make_args(tmp_path, secondary_dictionary=[tmp_path / "missing.json"])
        with pytest.raises(Exception, match="is not a file"):
            self.handle(args)

    def test_more_bridge_arguments_than_secondaries_rejected(self, tmp_path):
        secondary = write_dictionary(tmp_path / "ground.json", "Ground.Ground", 71)
        args = make_args(tmp_path, secondary_dictionary=[secondary],
                         secondary_bridge_arguments=["--communication-selection uart", "--communication-selection ip"])
        with pytest.raises(Exception, match="--secondary-bridge-arguments"):
            self.handle(args)

    def test_secondary_without_bridge_arguments_accepted(self, tmp_path):
        secondary = write_dictionary(tmp_path / "ground.json", "Ground.Ground", 71)
        args = self.handle(make_args(tmp_path, secondary_dictionary=[secondary]))
        assert args.secondary_dictionary == [secondary]


class TestDeploymentName:
    """deployment_name mirrors the root space system name fprime-to-xtce generates"""

    def test_dots_become_underscores(self, tmp_path):
        assert deployment_name(write_dictionary(tmp_path / "d.json", "Ref.Ref", 68)) == "Ref_Ref"

    def test_missing_deployment_name_rejected(self, tmp_path):
        path = tmp_path / "d.json"
        path.write_text(json.dumps({"metadata": {}}))
        with pytest.raises(ValueError, match="deploymentName"):
            deployment_name(path)


def instance_config():
    """A yamcs.<instance>.yaml configuration as the launcher leaves it after configuring the primary links"""
    return {
        "services": [{"class": "org.yamcs.ProcessRunner", "args": {"command": ["fprime-yamcs-events"]}}],
        "dataLinks": [
            {"name": "UDP_TM_IN", "class": TM_FRAME_LINK, "port": 50000, "frameType": "TM", "spacecraftId": 68,
             "frameLength": 248, "errorDetection": "CRC16", "encryption": [{"spi": 1}],
             "virtualChannels": [{"vcId": 1, "service": "PACKET", "stream": "tm_realtime", "encryptionSpis": [1],
                                  "packetPreprocessorArgs": {"doNotArchiveChannelIds": [5]}}]},
            {"name": "UDP_TM_SPLIT_IN", "class": TM_PACKET_LINK, "stream": "tm_realtime", "port": 50002,
             "packetPreprocessorArgs": {"doNotArchiveChannelIds": [5]}},
            {"name": "UDP_TC_OUT", "class": TC_FRAME_LINK, "host": "localhost", "port": 50001, "spacecraftId": 68,
             "maxFrameLength": 248, "virtualChannels": [{"vcId": 1, "stream": "tc_realtime", "encryptionSpi": 1}]},
        ],
        "mdb": [{"type": "xtce", "args": {"file": "/tmp/mdb/fprime.xtce.xml"}}],
        "streamConfig": {
            "tm": [{"name": "tm_realtime", "processor": "realtime"}, {"name": "tm_dump"}],
            "tc": [{"name": "tc_realtime", "processor": "realtime"}],
            "event": ["events_realtime"],
        },
    }


class TestSecondarySpacecraft:
    """Each secondary dictionary gets its own space system, frame links, streams and processors"""

    def configure(self, tmp_path, config, secondaries, primary=None):
        primary = primary or write_dictionary(tmp_path / "flight.json", "Ref.Ref", 68, 248)
        with patch.object(main_module.subprocess, "run") as run:
            configure_secondary_spacecraft(config, tmp_path / "mdb", primary, secondaries, 50001, 50000, 50002)
        return run

    def test_ports_follow_the_stride(self):
        assert secondary_ports(1, 50001, 50000, 50002) == (50011, 50010, 50012)
        assert secondary_ports(2, 50001, 50000, 50002) == (50001 + 2 * SECONDARY_PORT_STRIDE,
                                                           50000 + 2 * SECONDARY_PORT_STRIDE,
                                                           50002 + 2 * SECONDARY_PORT_STRIDE)

    def test_secondary_xtce_converted_and_loaded(self, tmp_path):
        config = instance_config()
        ground = write_dictionary(tmp_path / "ground.json", "Ground.Ground", 71, 1024)
        run = self.configure(tmp_path, config, [ground])
        xtce = tmp_path / "mdb" / "fprime-Ground_Ground.xtce.xml"
        run.assert_called_once_with([main_module.tool_path("fprime-to-xtce"), "-o", str(xtce), str(ground)], check=True)
        assert config["mdb"][-1] == {"type": "xtce", "args": {"file": str(xtce)}}

    def test_secondary_links_use_own_ports_scid_and_streams(self, tmp_path):
        config = instance_config()
        ground = write_dictionary(tmp_path / "ground.json", "Ground.Ground", 71, 1024)
        self.configure(tmp_path, config, [ground])
        links = {link["name"]: link for link in config["dataLinks"]}
        tm = links["UDP_TM_IN_Ground_Ground"]
        assert (tm["class"], tm["port"], tm["spacecraftId"], tm["frameLength"]) == (TM_FRAME_LINK, 50010, 71, 1024)
        assert tm["virtualChannels"][0]["stream"] == "tm_Ground_Ground"
        tc = links["UDP_TC_OUT_Ground_Ground"]
        assert (tc["class"], tc["port"], tc["spacecraftId"], tc["maxFrameLength"]) == (TC_FRAME_LINK, 50011, 71, 1024)
        assert tc["virtualChannels"][0]["stream"] == "tc_Ground_Ground"
        split = links["UDP_TM_SPLIT_IN_Ground_Ground"]
        assert (split["class"], split["port"], split["stream"]) == (TM_PACKET_LINK, 50012, "tm_Ground_Ground")
        # The primary links are left as configured
        assert (links["UDP_TM_IN"]["port"], links["UDP_TM_IN"]["spacecraftId"]) == (50000, 68)

    def test_primary_only_settings_not_copied(self, tmp_path):
        config = instance_config()
        self.configure(tmp_path, config, [write_dictionary(tmp_path / "ground.json", "Ground.Ground", 71)])
        links = {link["name"]: link for link in config["dataLinks"]}
        tm = links["UDP_TM_IN_Ground_Ground"]
        assert "encryption" not in tm and "encryptionSpis" not in tm["virtualChannels"][0]
        assert "packetPreprocessorArgs" not in tm["virtualChannels"][0]
        assert "encryptionSpi" not in links["UDP_TC_OUT_Ground_Ground"]["virtualChannels"][0]
        assert "packetPreprocessorArgs" not in links["UDP_TM_SPLIT_IN_Ground_Ground"]

    def test_streams_bound_to_space_systems(self, tmp_path):
        config = instance_config()
        self.configure(tmp_path, config, [write_dictionary(tmp_path / "ground.json", "Ground.Ground", 71)])
        tm = {entry["name"]: entry for entry in config["streamConfig"]["tm"]}
        tc = {entry["name"]: entry for entry in config["streamConfig"]["tc"]}
        assert tm["tm_realtime"]["rootContainer"] == f"/Ref_Ref/{XTCE_ROOT_CONTAINER}"
        assert tm["tm_Ground_Ground"] == {"name": "tm_Ground_Ground", "processor": "realtime",
                                          "rootContainer": f"/Ground_Ground/{XTCE_ROOT_CONTAINER}"}
        assert tc["tc_realtime"]["tcPatterns"] == ["/Ref_Ref/.*"]
        assert tc["tc_Ground_Ground"] == {"name": "tc_Ground_Ground", "processor": "realtime",
                                          "tcPatterns": ["/Ground_Ground/.*"]}
        assert "rootContainer" not in tm["tm_dump"]

    def test_secondary_processors_added(self, tmp_path):
        config = instance_config()
        ground = write_dictionary(tmp_path / "ground.json", "Ground.Ground", 71)
        self.configure(tmp_path, config, [ground])
        commands = [service["args"]["command"] for service in config["services"]
                    if service["class"] == "org.yamcs.ProcessRunner"]
        assert commands[0] == ["fprime-yamcs-events"]
        assert ["fprime-yamcs-events", "--dictionary", str(ground.resolve()),
                "--space-system", "Ground_Ground"] in commands
        assert ["fprime-yamcs-tlmchan", "--dictionary", str(ground.resolve()),
                "--space-system", "Ground_Ground", "--inject-port", "50012"] in commands
        for service in config["services"][1:]:
            assert service["args"]["restart"] == "always"

    def test_two_secondaries_get_distinct_ports(self, tmp_path):
        config = instance_config()
        self.configure(tmp_path, config, [write_dictionary(tmp_path / "a.json", "A.A", 71),
                                          write_dictionary(tmp_path / "b.json", "B.B", 72)])
        links = {link["name"]: link for link in config["dataLinks"]}
        assert links["UDP_TM_IN_A_A"]["port"] == 50010 and links["UDP_TM_IN_B_B"]["port"] == 50020
        assert links["UDP_TC_OUT_A_A"]["port"] == 50011 and links["UDP_TC_OUT_B_B"]["port"] == 50021

    def test_duplicate_deployment_name_rejected(self, tmp_path):
        with pytest.raises(Exception, match="deployment name"):
            self.configure(tmp_path, instance_config(), [write_dictionary(tmp_path / "g.json", "Ref.Ref", 71)])

    def test_duplicate_spacecraft_id_rejected(self, tmp_path):
        with pytest.raises(Exception, match="SpacecraftId"):
            self.configure(tmp_path, instance_config(), [write_dictionary(tmp_path / "g.json", "Ground.Ground", 68)])

    def test_missing_frame_link_rejected(self, tmp_path):
        config = instance_config()
        config["dataLinks"] = [link for link in config["dataLinks"] if link["class"] != TC_FRAME_LINK]
        with pytest.raises(Exception, match=TC_FRAME_LINK.replace(".", "\\.")):
            self.configure(tmp_path, config, [write_dictionary(tmp_path / "g.json", "Ground.Ground", 71)])

    def test_no_split_link_is_fine(self, tmp_path):
        config = instance_config()
        config["dataLinks"] = [link for link in config["dataLinks"] if link["class"] != TM_PACKET_LINK]
        self.configure(tmp_path, config, [write_dictionary(tmp_path / "g.json", "Ground.Ground", 71)])
        assert not any(link["name"].startswith("UDP_TM_SPLIT_IN") for link in config["dataLinks"])


class TestSecondaryCommBridges:
    """One fprime-comm-bridge per secondary with bridge arguments, on that secondary's YAMCS ports"""

    def args(self, tmp_path, **overrides):
        return make_args(tmp_path, udp_uplink_port=50001, udp_downlink_port=50000, udp_tm_inject_port=50002,
                         **overrides)

    def test_adapter_arguments_dictionary_and_ports(self, tmp_path):
        ground = tmp_path / "ground.json"
        arguments = secondary_comm_bridge_arguments(
            self.args(tmp_path), 1, ground, "--communication-selection uart --uart-device /dev/ttyACM1 --uart-baud 115200")
        assert arguments[:2] == ["--framing-selection", main_module.DEFAULT_FRAMING]
        assert contains_sequence(arguments, ["--communication-selection", "uart", "--uart-device", "/dev/ttyACM1",
                                             "--uart-baud", "115200"])
        assert contains_sequence(arguments, ["--dictionary", str(ground.resolve())])
        assert contains_sequence(arguments, ["--udp-fast-address", "127.0.0.1", "--udp-fast-send-port", "50010",
                                             "--udp-fast-bind-address", "127.0.0.1", "--udp-fast-recv-port", "50011"])

    def test_framing_selection_override_kept(self, tmp_path):
        arguments = secondary_comm_bridge_arguments(
            self.args(tmp_path), 2, tmp_path / "g.json", "--framing-selection tm-frame-aggregator --communication-selection ip")
        assert arguments.count("--framing-selection") == 1
        assert contains_sequence(arguments, ["--framing-selection", "tm-frame-aggregator"])
        assert contains_sequence(arguments, ["--udp-fast-send-port", "50020", "--udp-fast-bind-address", "127.0.0.1",
                                             "--udp-fast-recv-port", "50021"])

    def test_ground_side_options_dropped(self, tmp_path):
        arguments = secondary_comm_bridge_arguments(
            self.args(tmp_path), 1, tmp_path / "g.json", "--communication-selection ip --udp-fast-send-port 1 --ip-port 7")
        assert "1" not in arguments and contains_sequence(arguments, ["--ip-port", "7"])
        assert contains_sequence(arguments, ["--udp-fast-send-port", "50010"])

    def test_one_bridge_per_secondary_with_arguments(self, tmp_path):
        args = self.args(tmp_path, secondary_dictionary=[tmp_path / "a.json", tmp_path / "b.json"],
                         secondary_bridge_arguments=["--communication-selection ip"])
        with patch.object(main_module, "launch_process", return_value="process") as launch:
            assert launch_secondary_comm_bridges(args) == ["process"]
        command = launch.call_args.args[0]
        assert command[:len(main_module.BASE_MODULE_ARGUMENTS) + 1] == \
            main_module.BASE_MODULE_ARGUMENTS + ["fprime_gds.executables.comm_bridge"]
        assert contains_sequence(command, ["--dictionary", str((tmp_path / "a.json").resolve())])
        assert launch.call_args.kwargs["name"] == "fprime-comm-bridge[secondary 1]"

    def test_no_secondaries_launch_nothing(self, tmp_path):
        with patch.object(main_module, "launch_process") as launch:
            assert launch_secondary_comm_bridges(self.args(tmp_path)) == []
        launch.assert_not_called()


class TestGetDictionaryConstant:
    """get_dictionary_constant looks constants up by name"""

    def write_dictionary(self, tmp_path, constants):
        dictionary = tmp_path / "dictionary.json"
        dictionary.write_text(json.dumps({"constants": constants}))
        return dictionary

    def test_lookup_by_name(self, tmp_path):
        dictionary = self.write_dictionary(tmp_path, [
            {"qualifiedName": "ComCfg.SpacecraftId", "value": 68},
            {"qualifiedName": "Other", "value": 1},
            {"qualifiedName": "ComCfg.TmFrameFixedSize", "value": 1024},
        ])
        assert get_dictionary_constant(dictionary, "ComCfg.TmFrameFixedSize") == 1024
        assert get_dictionary_constant(dictionary, "ComCfg.SpacecraftId") == 68

    def test_missing_constant_rejected(self, tmp_path):
        dictionary = self.write_dictionary(tmp_path, [{"qualifiedName": "ComCfg.SpacecraftId", "value": 68}])
        with pytest.raises(ValueError, match="ComCfg.TmFrameFixedSize"):
            get_dictionary_constant(dictionary, "ComCfg.TmFrameFixedSize")


class TestAnchorRelativeMdbPaths:
    """anchor_relative_mdb_paths rewrites relative MDB file paths only"""

    def test_relative_path_anchored(self, tmp_path):
        config = {"mdb": [{"args": {"file": "mdb/fprime.xtce.xml"}}]}
        assert anchor_relative_mdb_paths(config, tmp_path) is True
        assert config["mdb"][0]["args"]["file"] == str((tmp_path / "mdb/fprime.xtce.xml").resolve())

    def test_absolute_path_untouched(self, tmp_path):
        absolute = str((tmp_path / "fprime.xtce.xml").resolve())
        config = {"mdb": [{"args": {"file": absolute}}]}
        assert anchor_relative_mdb_paths(config, tmp_path) is False
        assert config["mdb"][0]["args"]["file"] == absolute

    def test_no_mdb_section(self, tmp_path):
        assert anchor_relative_mdb_paths({}, tmp_path) is False


def parse_comm_args(*argv):
    """Parse launcher plugin and YAMCS arguments the way the launcher does, with a nominal dictionary path"""
    Plugins.system(LAUNCHER_PLUGIN_CATEGORIES)
    with patch.object(main_module, "discovered_web_extension_dirs", return_value=[]):
        with patch.object(sys, "argv", ["fprime-yamcs", *argv]):
            args, _ = ParserBase.parse_args([YamcsPluginArgumentParser, YamcsParser], "test")
    args.dictionary = DICTIONARY
    return args


def contains_sequence(arguments, expected):
    """True when the expected argument sequence appears contiguously in the arguments"""
    return any(arguments[i:i + len(expected)] == expected for i in range(len(arguments)))


class TestSecondaryArguments:
    """--secondary-dictionary / --secondary-bridge-arguments take any number of entries, as several values or
    repeated flags, in order"""

    @staticmethod
    def dictionaries(tmp_path, *names):
        paths = [tmp_path / name for name in names]
        for path in paths:
            path.write_text("{}")
        return paths

    def test_several_dictionaries_after_one_flag(self, tmp_path):
        a, b, c = self.dictionaries(tmp_path, "a.json", "b.json", "c.json")
        args = parse_comm_args("--secondary-dictionary", str(a), str(b), str(c))
        assert args.secondary_dictionary == [a, b, c]

    def test_repeated_flags_accumulate_in_order(self, tmp_path):
        a, b, c = self.dictionaries(tmp_path, "a.json", "b.json", "c.json")
        args = parse_comm_args("--secondary-dictionary", str(a), "--secondary-dictionary", str(b), str(c),
                               "--secondary-bridge-arguments", "--uart-device /dev/ttyA",
                               "--secondary-bridge-arguments", "--uart-device /dev/ttyB")
        assert args.secondary_dictionary == [a, b, c]
        assert args.secondary_bridge_arguments == ["--uart-device /dev/ttyA", "--uart-device /dev/ttyB"]

    def test_defaults_empty(self):
        args = parse_comm_args()
        assert args.secondary_dictionary == []
        assert args.secondary_bridge_arguments == []


class TestCommBridgeAutostart:
    """The launcher bridges every communication adapter other than udp through fprime-comm-bridge"""

    def test_default_selection_is_tcp_fast_server_on_gds_port(self):
        args = parse_comm_args()
        assert args.communication_selection == "tcp-fast-server"
        assert args.tcp_fast_port == 50000
        assert needs_comm_bridge(args.communication_selection) is True

    def test_udp_selection_needs_no_bridge(self):
        args = parse_comm_args("--communication-selection", "udp")
        assert needs_comm_bridge(args.communication_selection) is False

    @pytest.mark.parametrize("selection", ["tcp-fast-server", "tcp-fast-client", "ip", "uart"])
    def test_non_udp_selection_needs_bridge(self, selection):
        assert needs_comm_bridge(selection) is True

    def test_none_selection_needs_no_bridge(self):
        assert needs_comm_bridge("none") is False

    def test_launcher_exposes_only_communication_plugins(self):
        args = parse_comm_args()
        assert not hasattr(args, "framing_selection")

    def test_default_bridge_arguments(self):
        arguments = comm_bridge_arguments(parse_comm_args())
        assert arguments[:2] == ["--communication-selection", "tcp-fast-server"]
        for expected in (["--tcp-fast-port", "50000"],
                         ["--framing-selection", "tm-frame-aggregator"],
                         ["--dictionary", str(DICTIONARY)],
                         ["--udp-fast-address", "127.0.0.1", "--udp-fast-send-port", "50000"],
                         ["--udp-fast-bind-address", "127.0.0.1", "--udp-fast-recv-port", "50001"]):
            assert contains_sequence(arguments, expected), expected
        assert arguments[-8:] == ["--udp-fast-address", "127.0.0.1", "--udp-fast-send-port", "50000",
                                  "--udp-fast-bind-address", "127.0.0.1", "--udp-fast-recv-port", "50001"]

    def test_bridge_arguments_carry_tcp_fast_options(self):
        args = parse_comm_args("--communication-selection", "tcp-fast-client", "--tcp-fast-address", "10.0.0.7",
                               "--tcp-fast-port", "50123", "--udp-downlink-port", "60000")
        arguments = comm_bridge_arguments(args)
        for expected in (["--communication-selection", "tcp-fast-client"], ["--tcp-fast-address", "10.0.0.7"],
                         ["--tcp-fast-port", "50123"], ["--udp-fast-send-port", "60000"],
                         ["--udp-fast-recv-port", "50001"]):
            assert contains_sequence(arguments, expected), expected

    def test_bridge_arguments_reproduce_adapter_and_yamcs_ports(self):
        args = parse_comm_args("--communication-selection", "ip", "--ip-port", "50050", "--ip-client",
                               "--udp-downlink-port", "60000", "--udp-uplink-port", "60001")
        arguments = comm_bridge_arguments(args)
        for expected in (["--communication-selection", "ip"], ["--ip-port", "50050"], ["--ip-client"],
                         ["--framing-selection", "tm-frame-aggregator"],
                         ["--udp-fast-address", "127.0.0.1", "--udp-fast-send-port", "60000"],
                         ["--udp-fast-bind-address", "127.0.0.1", "--udp-fast-recv-port", "60001"]):
            assert contains_sequence(arguments, expected), expected

    def test_bridge_arguments_carry_uart_options(self):
        args = parse_comm_args("--communication-selection", "uart", "--uart-device", "/dev/ttyUSB3",
                               "--uart-baud", "115200")
        arguments = comm_bridge_arguments(args)
        assert arguments[:2] == ["--communication-selection", "uart"]
        assert arguments[arguments.index("--uart-device") + 1] == "/dev/ttyUSB3"
        assert arguments[arguments.index("--uart-baud") + 1] == "115200"

    def test_bridge_ground_options_forwarded_once(self):
        """The launcher's reproduced udp-fast defaults must not precede the bridge's explicit ground options"""
        arguments = comm_bridge_arguments(parse_comm_args("--udp-downlink-port", "60000"))
        for option in ("--udp-fast-address", "--udp-fast-send-port", "--udp-fast-bind-address",
                       "--udp-fast-recv-port"):
            assert arguments.count(option) == 1, option
        assert "--udp-fast-allowed-source" not in arguments
        assert arguments[arguments.index("--udp-fast-send-port") + 1] == "60000"

    def test_without_options_drops_option_and_values(self):
        arguments = ["--communication-selection", "uart", "--udp-fast-allowed-source", "10.0.0.1", "10.0.0.2",
                     "--uart-baud", "9600", "--udp-fast-send-port", "50000", "--ip-client"]
        assert without_options(arguments, "--udp-fast-") == ["--communication-selection", "uart", "--uart-baud",
                                                              "9600", "--ip-client"]

    def test_udp_fast_selection_rejected(self):
        with pytest.raises(Exception, match="YAMCS-side adapter"):
            check_communication_selection("udp-fast")

    @pytest.mark.parametrize("selection", ["udp", "none", "tcp-fast-server", "uart"])
    def test_other_selections_accepted(self, selection):
        check_communication_selection(selection)

    def test_ip_port_colliding_with_yamcs_rejected(self):
        args = parse_comm_args("--communication-selection", "ip", "--ip-port", "50001")
        with pytest.raises(Exception, match="collides with YAMCS --udp-uplink-port"):
            check_comm_bridge_ports(args)

    def test_distinct_ip_port_accepted(self):
        check_comm_bridge_ports(parse_comm_args("--communication-selection", "ip", "--ip-port", "50050"))

    def test_tcp_fast_port_may_match_yamcs_udp_port(self):
        """TCP and UDP port spaces are disjoint, so the default TCP 50000 coexists with the UDP TM intake on 50000"""
        check_comm_bridge_ports(parse_comm_args())

    @pytest.mark.parametrize("argv, expected", [
        ([], ("127.0.0.1", 50000)),
        (["--tcp-fast-address", "192.168.1.5", "--tcp-fast-port", "50123"], ("192.168.1.5", 50123)),
        (["--communication-selection", "ip", "--ip-address", "192.168.1.5", "--ip-port", "50050"],
         ("192.168.1.5", 50050)),
    ])
    def test_deployment_app_connects_to_hosting_adapter(self, argv, expected):
        args = parse_comm_args(*argv)
        with patch.object(main_module, "launch_app") as launch:
            launch_deployment_app(args)
        assert launch.call_args.args == (args, expected)

    def test_sdls_key_file_does_not_alter_deployment_arguments(self, tmp_path):
        """The key file configures YAMCS only; the deployment's -k comes from --application-arguments"""
        key_file = tmp_path / "sdls.key"
        key_file.write_bytes(bytes(32))
        args = parse_comm_args("--communication-selection", "udp", "--yamcs-sdls-key-file", str(key_file))
        args.application_arguments = None
        with patch.object(main_module, "launch_app") as launch:
            launch_deployment_app(args)
        assert args.application_arguments is None
        assert launch.call_args.args == (args, None)

    def test_launch_runs_comm_module(self):
        args = parse_comm_args("--communication-selection", "ip", "--ip-port", "50050")
        with patch.object(main_module, "launch_process") as launch:
            launch_comm_bridge(args)
        command = launch.call_args.args[0]
        assert command[:4] == [sys.executable, "-u", "-m", "fprime_gds.executables.comm_bridge"]
        assert command[4:] == comm_bridge_arguments(args)
        assert launch.call_args.kwargs["name"] == "fprime-comm-bridge[ip]"


class TestMavenFallback:
    """launch_yamcs_maven rejects unsupported configurations before launching"""

    def test_plugin_jars_rejected(self, tmp_path):
        args = make_args(tmp_path)
        with pytest.raises(Exception, match="--yamcs-plugin-jars is not supported"):
            launch_yamcs_maven(args, {}, [], [tmp_path / "plugin.jar"], Exception("reason"))

    def test_missing_maven_rejected(self, tmp_path):
        args = make_args(tmp_path)
        with patch.object(main_module.shutil, "which", return_value=None):
            with pytest.raises(Exception, match="install Maven"):
                launch_yamcs_maven(args, {}, [], [], Exception("reason"))
