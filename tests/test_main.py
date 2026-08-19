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
from argparse import Namespace
from unittest.mock import patch

import pytest

from fprime_yamcs import __main__ as main_module
from fprime_yamcs.__main__ import YamcsParser, anchor_relative_mdb_paths, launch_yamcs_maven


def make_args(tmp_path, **overrides):
    """Build a parsed-argument namespace with valid defaults"""
    values = {
        "yamcs_config_dir": tmp_path,
        "yamcs_web_extension_dirs": [],
        "yamcs_plugin_jars": [],
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
