""" F Prime YAMCS

This script is designed to replace fprime-gds with a YAMCS based GDS. It will start YAMCS with the F Prime Event
Processor.

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
import atexit
import fnmatch
import os
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import webbrowser

import yaml

from importlib.resources import files
from typing import Any, Dict, List, Tuple
from pathlib import Path

from fprime_gds.executables.cli import ConfigDrivenParser, DictionaryParser, BinaryDeployment, LogDeployParser, ParserBase, PluginArgumentParser
from fprime_gds.executables.run_deployment import launch_app, launch_process

from fprime_yamcs.java import (
    JavaResolutionException,
    build_classpath,
    discovered_plugin_jars,
    discovered_web_extension_dirs,
    expand_jar_arguments,
    find_java,
    yamcs_launch_command,
)

class YamcsParser(ParserBase):
    """Parser for YAMCS specific arguments"""

    DESCRIPTION = "YAMCS settings for use with F Prime"

    def get_arguments(self) -> Dict[Tuple[str, ...], Dict[str, Any]]:
        """Arguments to handle deployments"""
        return {
            ("-g", "--gui"): {
                "choices": ["none", "html"],
                "dest": "gui",
                "type": str,
                "default": "html",
                "help": "Set the desired GUI system for running the deployment. [default: %(default)s]",
            },
            ("--skip-browser-open",): {
                "dest": "browser_auto_open",
                "action": "store_false",
                "help": "Run YAMCS without auto-launching the default web browser",
            },
            ("--yamcs-config-dir",): {
                "action": "store",
                "default": Path(__file__).resolve().parent / "yamcs" / "src" / "main" / "yamcs",
                "type": Path,
                "help": "Specify the YAMCS configuration directory. Default: %(default)s",
            },
            ("--yamcs-data-dir",): {
                "action": "store",
                "default": Path(os.getcwd()).joinpath("yamcs-data"),
                "type": Path,
                "help": "Specify the YAMCS data directory. Default: %(default)s",
            },
            ("--yamcs-events-instance",): {
                "action": "store",
                "default": None,
                "type": Path,
                "help": "Specify the YAMCS instance to use for fprime-events",
            },
            ("--yamcs-realtime-only-channels",): {
                "action": "store",
                "nargs": "+",
                "default": [],
                "metavar": "CHANNEL",
                "help": "Telemetry channel names (fnmatch globs allowed, e.g. 'Deployment.camera.FrameOut*') kept "
                        "realtime-only: their packets are not recorded and never enter the parameter archive.",
            },
            ("--yamcs-web-extension-dirs",): {
                "action": "store",
                "nargs": "+",
                "default": [],
                "type": Path,
                "metavar": "DIR",
                "help": "Directories containing yamcs-web extensions. Every .js file in a directory is "
                        "loaded as a module script by the YAMCS web interface. Extensions shipped by "
                        "installed pip packages (fprime_yamcs.web_extensions entry points) are added "
                        "automatically.",
            },
            ("--yamcs-plugin-jars",): {
                "action": "store",
                "nargs": "+",
                "default": [],
                "type": Path,
                "metavar": "JAR_OR_DIR",
                "help": "Extra YAMCS plugin jars (or directories of jars) appended to the YAMCS "
                        "classpath. Plugin jars shipped by installed pip packages "
                        "(fprime_yamcs.plugin_jars entry points) are added automatically.",
            },
            ("--udp-uplink-port", ): {
                "action": "store",
                "default": 50001,
                "type": int,
                "help": "Specify the UDP port for uplink (TC) communication with YAMCS. Default: %(default)s",
            },
            ("--udp-downlink-port", ): {
                "action": "store",
                "default": 50000,
                "type": int,
                "help": "Specify the UDP port for downlink (TM) communication with YAMCS. Default: %(default)s",
            },
            ("--udp-tm-inject-port", ): {
                "action": "store",
                "default": 50002,
                "type": int,
                "help": "Specify the UDP port for re-injecting split telemetry channel packets into YAMCS. "
                        "Default: %(default)s",
            },
        }

    def handle_arguments(self, args, **kwargs):
        """Handle arguments as parsed"""
        if args.yamcs_config_dir is not None and not args.yamcs_config_dir.is_dir():
            raise Exception(f"[ERROR] YAMCS config {args.yamcs_config_dir} is not a directory.")
        # User-supplied extension dirs fail fast; auto-discovered ones are skipped with a
        # warning so an installed package cannot render the launcher unable to start
        for extension_dir in args.yamcs_web_extension_dirs:
            if not extension_dir.is_dir():
                raise Exception(f"[ERROR] YAMCS web extension {extension_dir} is not a directory.")
            resolved = str(extension_dir.absolute())
            if "," in resolved or any(character.isspace() for character in resolved):
                raise Exception(f"[ERROR] YAMCS web extension path may not contain commas or whitespace: {resolved}")
        discovered = []
        for extension_dir in discovered_web_extension_dirs():
            resolved = str(extension_dir.absolute())
            if "," in resolved or any(character.isspace() for character in resolved):
                print(f"[WARNING] Skipping discovered web extension (comma/whitespace in path): {resolved}",
                      file=sys.stderr)
                continue
            discovered.append(extension_dir)
        args.yamcs_web_extension_dirs = list(args.yamcs_web_extension_dirs) + discovered
        for plugin_jar in args.yamcs_plugin_jars:
            if not plugin_jar.exists():
                raise Exception(f"[ERROR] YAMCS plugin jar {plugin_jar} does not exist.")
            if plugin_jar.is_dir() and not sorted(plugin_jar.glob("*.jar")):
                print(f"[WARNING] YAMCS plugin jar directory {plugin_jar} contains no *.jar files.",
                      file=sys.stderr)
        return args

def yamcs_instances(config_directory: Path) -> List[str]:
    """ Load the YAMCS instance names from the configuration directory

    This reads instance configurations from "etc/yamcs.yml" under the supplied configuration directory and extracts the
    instance names from the instance list.

    Args:
        config_directory: The YAMCS configuration directory to search for instance configurations
    Returns:
        A list of YAMCS instance names found in the configuration directory
    """
    yamcs_yaml =  config_directory / "etc" / "yamcs.yaml"
    if not yamcs_yaml.is_file():
        raise Exception(f"YAMCS configuration {yamcs_yaml} not found.")
    try:
        with yamcs_yaml.open() as f:
            instance_config = yaml.safe_load(f)
    except Exception as exc:
        raise Exception(f"Failed to read YAMCS configuration {yamcs_yaml}: {exc}")
    try:
        return instance_config["instances"]
    except KeyError:
        raise Exception(f"No instances found in YAMCS configuration {yamcs_yaml}")


def xtce_mdb_location(config_directory: Path, instances: List[str]) -> Tuple[Path, str]:
    """ Load the YAMCS XTCE MDB location from the instance configuration

    YAMCS allows multiple instances with multiple MDBs. This function looks for the first MDB instance of type "xtce"
    and a file argument that ends with "fprime.xtce.xml". This is the XTCE XML that should be updated from the F Prime
    dictionary.

    This reads instance configurations from "etc/yamcs.*.yml"

    Args:
        config_directory: The YAMCS configuration directory to search for instance configurations
        instances: A list of YAMCS instance names to consider
    Returns:
        The path to the XTCE MDB file to update and the instance it was found in
    """
    # Read the instance file to find the MDB location
    for instance in instances:
        instance_path = config_directory / "etc" / f"yamcs.{instance}.yaml"
        if not instance_path.is_file():
            print(f"[WARNING] YAMCS instance configuration {instance_path} not found. Skipping.",
                  file=sys.stderr)
            continue
        try:
            with instance_path.open() as f:
                instance_config = yaml.safe_load(f)
        except Exception as exc:
            print(f"[WARNING] Failed to read YAMCS instance configuration {instance_path}: {exc}",
                  file=sys.stderr)
            continue
        for mdb in instance_config.get("mdb", {}):
            mdb_type = mdb.get("type", None)
            file_path = mdb.get("args", {}).get("file", None)
            if mdb_type != "xtce" or file_path is None or not file_path.endswith("fprime.xtce.xml"):
                print(f"[WARNING] Skipping non-fprime '{mdb_type}' MDB in {instance_path}",
                      file=sys.stderr)
                continue
            return config_directory / file_path, instance
    else:
        raise Exception(f"No valid YAMCS instance found in {config_directory / 'etc'}")

def get_dictionary_constants(dictionary: Path, constants: List[str]) -> str:
    """ Get the dictionary constant from the F Prime dictionary path

    This extracts constants from the F Prime dictionary.

    Args:
        dictionary: The path to the F Prime dictionary file
        constants: A list of constant names to look for in the dictionary
    Returns:
        a list of constants found in the dictionary that match the supplied list of constant names
    """
    with open(str(dictionary)) as f:
        dictionary_data = json.load(f)
        constants_data = dictionary_data.get("constants", [])
    found_constants = [
        constant["value"] for constant in constants_data if constant.get("qualifiedName", "") in constants
    ]
    if len(found_constants) != len(constants):
        raise ValueError(f"Required constants {constants} not found in dictionary")
    return found_constants



def get_channel_ids(dictionary: Path, channel_patterns: List[str]) -> List[int]:
    """ Resolve telemetry channel name patterns to channel ids

    Matches each supplied pattern (fnmatch glob) against the qualified telemetry channel names in the F Prime
    dictionary and returns the ids of all matching channels.

    Args:
        dictionary: The path to the F Prime dictionary file
        channel_patterns: A list of channel name patterns (fnmatch globs)
    Returns:
        A sorted list of matching telemetry channel ids
    """
    with open(str(dictionary)) as f:
        channels = json.load(f).get("telemetryChannels", [])
    channel_ids = set()
    for pattern in channel_patterns:
        matches = [channel["id"] for channel in channels if fnmatch.fnmatchcase(channel["name"], pattern)]
        if not matches:
            raise ValueError(f"Realtime-only channel pattern '{pattern}' matched no telemetry channels")
        channel_ids.update(matches)
    return sorted(channel_ids)


def get_packet_ids(dictionary: Path, channel_patterns: List[str]) -> List[int]:
    """ Resolve telemetry channel name patterns to packetized-telemetry packet ids

    Deployments using Svc.TlmPacketizer downlink telemetry as packets (APID 4) rather than
    individual channel samples (APID 1). Any packet containing a channel that matches one of
    the supplied patterns is treated as realtime-only.

    Args:
        dictionary: The path to the F Prime dictionary file
        channel_patterns: A list of channel name patterns (fnmatch globs)
    Returns:
        A sorted list of packet ids whose member channels match any pattern
    """
    with open(str(dictionary)) as f:
        packet_sets = json.load(f).get("telemetryPacketSets", [])
    packet_ids = set()
    for packet_set in packet_sets:
        for packet in packet_set.get("members", []):
            if any(fnmatch.fnmatchcase(member, pattern)
                   for member in packet.get("members", [])
                   for pattern in channel_patterns):
                packet_ids.add(packet["id"])
    return sorted(packet_ids)


def anchor_relative_mdb_paths(instance_config: dict, base_directory: Path) -> bool:
    """ Anchor relative MDB file paths in an instance configuration to a base directory

    Args:
        instance_config: a parsed yamcs.<instance>.yaml configuration
        base_directory: the directory relative MDB paths are resolved against
    Returns:
        True when at least one path was rewritten
    """
    changed = False
    for mdb in instance_config.get("mdb", []):
        file_path = mdb.get("args", {}).get("file", None)
        if file_path is not None and not Path(file_path).is_absolute():
            mdb["args"]["file"] = str((base_directory / file_path).resolve())
            changed = True
    return changed


def construct_temporary_configuration(config_directory: Path, instances: List[str], dictionary: Path, uplink_port: int, downlink_port: int, tm_inject_port: int, realtime_only_channels: List[str]) -> Tuple[Path, str]:
    """ Construct a temporary YAMCS configuration directory

    The YAMCS configuration that ships with fprime-yamcs needs to be modified in several specific ways before running
    with YAMCS. These include:
        1. Updating the XTCE MDB file with the converted F Prime dictionary
        2. Updating the TM/TC processors to use the correct UDP ports
        3. Updating the TM/TC processors to use the correct dictionary constants
        4. Marking realtime-only telemetry channels as "do not archive" and switching the parameter
           archive to backfilling so those channels never reach the archives
    Args:
        config_directory: The YAMCS configuration directory to use as a base for the temporary configuration
        instances: A list of YAMCS instance names to consider for configuration
        dictionary: The path to the F Prime dictionary file to convert and use for the XTCE MDB
        uplink_port: The UDP port to use for uplink (TC) communication with YAMCS
        downlink_port: The UDP port to use for downlink (TM) communication with YAMCS
        tm_inject_port: The UDP port to use for re-injecting split telemetry channel packets
        realtime_only_channels: Telemetry channel name patterns to keep realtime-only (not archived)
    Returns:
        The path to the temporary YAMCS configuration directory and the fprime identified instance
    """

    # Create a temporary configuration directory that will be destroyed on exit
    yamcs_working_config_dir = Path(tempfile.mkdtemp())
    atexit.register(lambda: shutil.rmtree(yamcs_working_config_dir))

    # Copy the default configuration to the temporary directory
    shutil.copytree(config_directory, yamcs_working_config_dir, dirs_exist_ok=True)
    xtce_dictionary, fprime_instance = xtce_mdb_location(yamcs_working_config_dir, instances)

    print(f"[INFO] Updating YAMCS XTCE dictionary from {dictionary} to {xtce_dictionary}")
    subprocess.run(["fprime-to-xtce", "-o", str(xtce_dictionary), str(dictionary)], check=True)

    # YAMCS is launched without a controlled working directory, so relative MDB paths in
    # every instance configuration must be anchored to the temporary configuration directory
    for other_instance_path in sorted((yamcs_working_config_dir / "etc").glob("yamcs.*.yaml")):
        with other_instance_path.open() as f:
            other_instance_config = yaml.safe_load(f)
        if anchor_relative_mdb_paths(other_instance_config, yamcs_working_config_dir):
            with other_instance_path.open("w") as f:
                yaml.safe_dump(other_instance_config, f)

    print("[INFO] Setting ports for YAMCS UDP processors")
    instance_path = yamcs_working_config_dir / "etc" / f"yamcs.{fprime_instance}.yaml"
    assert instance_path.is_file(), f"YAMCS instance configuration {instance_path} not found."
    with instance_path.open() as f:
        instance_config = yaml.safe_load(f)
    constants = get_dictionary_constants(dictionary, ["ComCfg.TmFrameFixedSize", "ComCfg.SpacecraftId"])
    realtime_only_ids = get_channel_ids(dictionary, realtime_only_channels)
    realtime_only_packet_ids = get_packet_ids(dictionary, realtime_only_channels) if realtime_only_channels else []
    for link in instance_config.get("dataLinks", []):
        print(link)
        if link.get("class", "") == "org.yamcs.tctm.ccsds.UdpTmFrameLink":
            print(f"[INFO] Setting downlink port for TM link {link.get('name', '')} to {downlink_port}")
            link["port"] = downlink_port
            link["frameLength"] = constants[0]
            link["spacecraftId"] = constants[1]
            for vc in link.get("virtualChannels", []):
                # Space packets may span multiple TM frames (e.g. large telemetry
                # channels), so the maximum packet length is independent of (and can
                # exceed) the frame size. Allow the CCSDS maximum: 65536 + 6 header bytes.
                vc["maxPacketLength"] = 65542
                if realtime_only_ids:
                    vc.setdefault("packetPreprocessorArgs", {})["doNotArchiveChannelIds"] = realtime_only_ids
                if realtime_only_packet_ids:
                    vc.setdefault("packetPreprocessorArgs", {})["doNotArchivePacketIds"] = realtime_only_packet_ids
        elif link.get("class", "") == "org.yamcs.tctm.UdpTmDataLink":
            print(f"[INFO] Setting split telemetry injection port for TM link {link.get('name', '')} to {tm_inject_port}")
            link["port"] = tm_inject_port
            if realtime_only_ids:
                link.setdefault("packetPreprocessorArgs", {})["doNotArchiveChannelIds"] = realtime_only_ids
            if realtime_only_packet_ids:
                link.setdefault("packetPreprocessorArgs", {})["doNotArchivePacketIds"] = realtime_only_packet_ids
        elif link.get("class", "") == "org.yamcs.tctm.ccsds.UdpTcFrameLink":
            print(f"[INFO] Setting downlink port for TM link {link.get('name', '')} to {downlink_port}")
            link["port"] = uplink_port
            link["maxFrameLength"] = constants[0]
            link["spacecraftId"] = constants[1]
    if realtime_only_ids:
        print(f"[INFO] Keeping {len(realtime_only_ids)} telemetry channels "
              f"and {len(realtime_only_packet_ids)} telemetry packets realtime-only (not archived)")
        # The realtime filler archives parameters straight off the realtime processor, which would
        # bypass the "do not archive" packet flag. Switch to backfilling from the recorded tm table
        # (where the flagged packets are absent) so the excluded channels never reach the archive.
        for service in instance_config.get("services", []):
            if service.get("class", "") == "org.yamcs.parameterarchive.ParameterArchive":
                args = service.setdefault("args", {})
                args.setdefault("realtimeFiller", {})["enabled"] = False
                args.setdefault("backFiller", {})["automaticBackfilling"] = True
    with instance_path.open("w") as f:
        yaml.safe_dump(instance_config, f)

    return  yamcs_working_config_dir, fprime_instance


def yamcs_web_url(config_directory: Path) -> str:
    """ Determine the YAMCS web interface URL from the YAMCS configuration

    Reads the HttpServer service port from "etc/yamcs.yaml" under the supplied configuration directory,
    falling back to the YAMCS default port (8090).

    Args:
        config_directory: The YAMCS configuration directory
    Returns:
        The URL of the YAMCS web interface
    """
    port = 8090
    yamcs_yaml = config_directory / "etc" / "yamcs.yaml"
    try:
        with yamcs_yaml.open() as f:
            for service in yaml.safe_load(f).get("services", []):
                if service.get("class", "").endswith(".HttpServer"):
                    port = service.get("args", {}).get("port", port)
                    break
    except Exception as exc:
        print(f"[WARNING] Failed to read HTTP port from {yamcs_yaml}: {exc}", file=sys.stderr)
    return f"http://127.0.0.1:{port}/"


def launch_browser(parsed_args):
    """ Open the YAMCS web interface in the default browser once it responds

    Polls the YAMCS web interface in a background thread and opens the default web browser once YAMCS is serving,
    mirroring the fprime-gds HTML GUI auto-open behavior.

    Args:
        parsed_args: parsed argument namespace
    """
    ui_url = yamcs_web_url(parsed_args.yamcs_config_dir)

    def poll_and_open():
        """Wait for the web interface to serve, then open the browser"""
        for _ in range(120):
            try:
                urllib.request.urlopen(ui_url, timeout=1)
                break
            except Exception:
                time.sleep(1)
        print(f"[INFO] Launched UI at: {ui_url}")
        if parsed_args.browser_auto_open:
            webbrowser.open(ui_url, new=0, autoraise=True)

    threading.Thread(target=poll_and_open, daemon=True).start()


def launch_yamcs(parsed_args):
    """ Launch YAMCS """
    # Set up the environment variables required by YAMCS and fprime-yamcs
    environment = os.environ.copy()
    environment["FPRIME_DICTIONARY"] = parsed_args.dictionary
    environment["FPRIME_YAMCS_INSTANCE"] = parsed_args.yamcs_events_instance
    environment["FPRIME_YAMCS_TM_INJECT_PORT"] = str(parsed_args.udp_tm_inject_port)

    print(f"[INFO] Using FPRIME_DICTIONARY: {environment['FPRIME_DICTIONARY']}")
    print(f"[INFO] Using FPRIME_YAMCS_INSTANCE: {environment['FPRIME_YAMCS_INSTANCE']}")
    print(f"[INFO] Using YAMCS_DATA_DIR: {parsed_args.yamcs_data_dir.absolute()}")
    print(f"[INFO] Using YAMCS_CONFIG_DIR: {parsed_args.yamcs_config_dir.absolute()}")

    # High-rate telemetry (many packets/s) needs more heap than the JVM
    # default of 1/4 of physical RAM allows on small machines.
    jvm_args = ["-Xmx4g"]
    if parsed_args.yamcs_web_extension_dirs:
        extension_dirs = ",".join(str(d.absolute()) for d in parsed_args.yamcs_web_extension_dirs)
        jvm_args.append(f"-Dfprime.yamcs.webExtensions={extension_dirs}")

    plugin_jars = expand_jar_arguments(parsed_args.yamcs_plugin_jars)
    try:
        classpath = build_classpath(plugin_jars)
        java = find_java()
    except JavaResolutionException as exc:
        return launch_yamcs_maven(parsed_args, environment, jvm_args, plugin_jars, exc)

    parsed_args.yamcs_data_dir.mkdir(parents=True, exist_ok=True)
    return launch_process(
        yamcs_launch_command(java, classpath,
                             parsed_args.yamcs_config_dir.absolute() / "etc",
                             parsed_args.yamcs_data_dir.absolute(), jvm_args),
        name="YAMCS", env=environment)


def launch_yamcs_maven(parsed_args, environment, jvm_args, plugin_jars, reason: Exception):
    """ Launch YAMCS through Maven (fallback for source checkouts without prebuilt jars) """
    if plugin_jars:
        raise Exception(f"[ERROR] {reason} --yamcs-plugin-jars is not supported with the Maven fallback.")
    if shutil.which("mvn") is None:
        raise Exception(f"[ERROR] {reason} Alternatively install Maven (mvn) to build and run from source.")
    print(f"[WARNING] {reason} Falling back to Maven.", file=sys.stderr)
    discovered = discovered_plugin_jars()
    if discovered:
        print(f"[WARNING] {len(discovered)} entry-point plugin jar(s) will not be loaded under "
              "the Maven fallback.", file=sys.stderr)
    return launch_process(
        ["mvn", "-f", str(Path(__file__).resolve().parent / "yamcs" / "pom.xml"), "yamcs:run",
         f"-Dyamcs.configurationDirectory={parsed_args.yamcs_config_dir.absolute()}",
         f"-Dyamcs.directory={parsed_args.yamcs_data_dir.absolute()}",
         f"-Dyamcs.jvmArgs={' '.join(jvm_args)}"
        ],
                          env=environment)


def parse_args():
    """ Parse the arguments for F Prime YAMCS"""
    argument_handlers = [
        DictionaryParser,
        BinaryDeployment,
        LogDeployParser,
        PluginArgumentParser,
        YamcsParser
    ]
    # If the FPRIME_GDS_CONFIG_PATH environment variable is set, use it as the default configuration path
    if "FPRIME_GDS_CONFIG_PATH" in os.environ:
        ConfigDrivenParser.set_default_configuration(
            Path(os.environ["FPRIME_GDS_CONFIG_PATH"])
        )
    # Parse the arguments, and refine through all handlers
    args, _ = ConfigDrivenParser.parse_args(
        argument_handlers, "Run F prime deployment and GDS"
    )
    return args


def main():
    """ Main entrypoint for F Prime YAMCS

    This performs the argument processing, and then starts F Prime YAMCS.
    """
    parsed_args = parse_args()
    try:
        # First load the instances to find the XTCE MDB location, and convert the F Prime dictionary if needed
        instances = yamcs_instances(parsed_args.yamcs_config_dir)
        if not instances:
            raise Exception(f"No YAMCS instances found in {parsed_args.yamcs_config_dir / 'etc/yamcs.yaml'}")

        yamcs_config_dir, fprime_instance = construct_temporary_configuration(parsed_args.yamcs_config_dir, instances, parsed_args.dictionary, parsed_args.udp_uplink_port, parsed_args.udp_downlink_port, parsed_args.udp_tm_inject_port, parsed_args.yamcs_realtime_only_channels)
        parsed_args.yamcs_config_dir = yamcs_config_dir
        if parsed_args.yamcs_events_instance is None:
            parsed_args.yamcs_events_instance = fprime_instance
        launched_apps = [launch_app] if parsed_args.app is not None else []
        processes = [launcher(parsed_args) for launcher in launched_apps + [launch_yamcs]]
        if parsed_args.gui == "html":
            launch_browser(parsed_args)
        print("[INFO] F Prime/YAMCS is now running. CTRL-C to shutdown all components.")
        processes[-1].wait()
    except KeyboardInterrupt:
        print("[INFO] CTRL-C received. Exiting.")
    except Exception as exc:
        print(f"[INFO] Shutting down F Prime/YAMCS due to error. {str(exc)}", file=sys.stderr)
        return 1
    # Processes are killed atexit
    return 0


if __name__ == "__main__":
    main()
