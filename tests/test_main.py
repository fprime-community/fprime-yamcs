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
import yaml

from fprime_gds.executables.cli import ParserBase
from fprime_gds.plugin.system import Plugins

from fprime_yamcs import __main__ as main_module
from fprime_yamcs.__main__ import (
    LAUNCHER_PLUGIN_CATEGORIES,
    YamcsParser,
    YamcsPluginArgumentParser,
    anchor_relative_mdb_paths,
    check_comm_bridge_ports,
    check_communication_selection,
    comm_bridge_arguments,
    construct_temporary_configuration,
    get_dictionary_constant,
    launch_comm_bridge,
    launch_deployment_app,
    launch_yamcs_maven,
    needs_comm_bridge,
    without_options,
)

DICTIONARY = Path("/deploy/dict/RefTopologyDictionary.json")


def make_args(tmp_path, **overrides):
    """Build a parsed-argument namespace with valid defaults"""
    values = {
        "yamcs_config_dir": tmp_path,
        "yamcs_web_extension_dirs": [],
        "yamcs_plugin_jars": [],
        "yamcs_sdls_key_file": None,
        "yamcs_sdls_spi": 1,
    }
    values.update(overrides)
    return Namespace(**values)


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


class TestConstructTemporaryConfiguration:
    """Configuration rewrites must preserve the order used to initialize streams"""

    @pytest.mark.parametrize("rewrite_path", ["relative-mdb", "absolute-mdb", "other-instance"])
    def test_preserves_stream_config_order(self, tmp_path, rewrite_path):
        config_directory = tmp_path / "config"
        (config_directory / "etc").mkdir(parents=True)
        (config_directory / "mdb").mkdir()
        source_mdb = config_directory / "mdb" / "fprime.xtce.xml"
        source_mdb.write_text("<SpaceSystem name='test' />")
        sql_file = tmp_path / "extra_streams.sql"
        sql_file.write_text("create stream cfdp_in as select * from tm_realtime\n")
        stream_config = {
            "tm": [{"name": "tm_realtime"}],
            "tc": [{"name": "tc_realtime"}],
            "sqlFile": str(sql_file),
        }
        fprime_mdb = "mdb/fprime.xtce.xml" if rewrite_path == "relative-mdb" else str(source_mdb)
        instance_config = {
            "mdb": [{"type": "xtce", "args": {"file": fprime_mdb}}],
            "dataLinks": [
                {"class": "org.yamcs.tctm.ccsds.UdpTmFrameLink", "port": 50000},
                {"class": "org.yamcs.tctm.ccsds.UdpTcFrameLink", "port": 50001},
                {"class": "org.yamcs.tctm.UdpTmDataLink", "port": 50002},
            ],
            "streamConfig": stream_config,
        }
        instance_path = config_directory / "etc" / "yamcs.fprime-project.yaml"
        instance_path.write_text(yaml.safe_dump(instance_config, sort_keys=False))
        other_path = config_directory / "etc" / "yamcs.other.yaml"
        other_path.write_text(yaml.safe_dump({
            "mdb": [{"type": "xtce", "args": {"file": "mdb/other.xtce.xml"}}],
            "streamConfig": stream_config,
        }, sort_keys=False))
        originals = {path: path.read_bytes() for path in (instance_path, other_path, source_mdb, sql_file)}
        dictionary = tmp_path / "dictionary.json"
        dictionary.write_text(json.dumps({"constants": [
            {"qualifiedName": "ComCfg.TmFrameFixedSize", "value": 1024},
            {"qualifiedName": "ComCfg.SpacecraftId", "value": 68},
        ]}))

        # Exercise the real copy/load/rewrite path; only the external converter is bypassed.
        with patch.object(main_module.subprocess, "run") as convert:
            working_directory, instance = construct_temporary_configuration(
                config_directory, ["fprime-project"], dictionary,
                uplink_port=60001, downlink_port=60000, tm_inject_port=60002,
                realtime_only_channels=[],
            )

        assert instance == "fprime-project"
        expected_mdb = working_directory / "mdb/fprime.xtce.xml" if rewrite_path == "relative-mdb" else source_mdb
        convert.assert_called_once_with(["fprime-to-xtce", "-o", str(expected_mdb), str(dictionary)], check=True)
        target = "other" if rewrite_path == "other-instance" else instance
        rewritten = yaml.safe_load((working_directory / "etc" / f"yamcs.{target}.yaml").read_text())
        # Mapping equality alone would miss the ordering regression.
        assert list(rewritten["streamConfig"]) == list(stream_config)
        assert rewritten["streamConfig"] == stream_config
        target_mdb = working_directory / "mdb/other.xtce.xml" if target == "other" else expected_mdb
        assert rewritten["mdb"][0]["args"]["file"] == str(target_mdb)

        selected = yaml.safe_load((working_directory / "etc" / instance_path.name).read_text())
        tm_link, tc_link, inject_link = selected["dataLinks"]
        assert (tm_link["port"], tm_link["frameLength"], tm_link["spacecraftId"]) == (60000, 1024, 68)
        assert (tc_link["port"], tc_link["maxFrameLength"], tc_link["spacecraftId"]) == (60001, 1024, 68)
        assert inject_link["port"] == 60002
        assert {path: path.read_bytes() for path in originals} == originals


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
