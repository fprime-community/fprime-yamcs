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
import os
import json
import shutil
import subprocess
import sys
import tempfile

import yaml

from importlib.resources import files
from typing import Any, Dict, List, Tuple
from pathlib import Path

from fprime_gds.executables.cli import ConfigDrivenParser, DictionaryParser, BinaryDeployment, LogDeployParser, ParserBase, PluginArgumentParser
from fprime_gds.executables.run_deployment import launch_app, launch_process

class YamcsParser(ParserBase):
    """Parser for YAMCS specific arguments"""

    DESCRIPTION = "YAMCS settings for use with F Prime"

    def get_arguments(self) -> Dict[Tuple[str, ...], Dict[str, Any]]:
        """Arguments to handle deployments"""
        return {
            ("--yamcs-config-dir",): {
                "action": "store",
                "default": files("fprime_yamcs").joinpath("yamcs/src/main/yamcs"),
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
        }

    def handle_arguments(self, args, **kwargs):
        """Handle arguments as parsed"""
        if shutil.which("mvn") is None:
            raise Exception("[ERROR] Maven (mvn) is required. Please install and ensure it is in your PATH.")
        if args.yamcs_config_dir is not None and not args.yamcs_config_dir.is_dir():
            raise Exception(f"[ERROR] YAMCS config {args.yamcs_config_dir} is not a directory.")
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
    with dictionary.open() as f:
        dictionary_data = json.load(f)
        constants_data = dictionary_data.get("constants", [])
    found_constants = [
        constant["value"] for constant in constants_data if constant.get("qualifiedName", "") in constants
    ]
    if len(found_constants) != len(constants):
        raise ValueError(f"Required constants {constants} not found in dictionary")
    return found_constants


def construct_temporary_configuration(config_directory: Path, instances: List[str], dictionary: Path, uplink_port: int, downlink_port: int) -> Tuple[Path, str]:
    """ Construct a temporary YAMCS configuration directory

    The YAMCS configuration that ships with fprime-yamcs needs to be modified in several specific ways before running
    with YAMCS. These include:
        1. Updating the XTCE MDB file with the converted F Prime dictionary
        2. Updating the TM/TC processors to use the correct UDP ports
        3. Updating the TM/TC processors to use the correct dictionary constants
    Args:
        config_directory: The YAMCS configuration directory to use as a base for the temporary configuration
        instances: A list of YAMCS instance names to consider for configuration
        dictionary: The path to the F Prime dictionary file to convert and use for the XTCE MDB
        uplink_port: The UDP port to use for uplink (TC) communication with YAMCS
        downlink_port: The UDP port to use for downlink (TM) communication with YAMCS
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

    print("[INFO] Setting ports for YAMCS UDP processors")
    instance_path = yamcs_working_config_dir / "etc" / f"yamcs.{fprime_instance}.yaml"
    assert instance_path.is_file(), f"YAMCS instance configuration {instance_path} not found."
    with instance_path.open() as f:
        instance_config = yaml.safe_load(f)
    constants = get_dictionary_constants(dictionary, ["ComCfg.TmFrameFixedSize", "ComCfg.SpacecraftId"])
    for link in instance_config.get("dataLinks", []):
        print(link)
        if link.get("class", "") == "org.yamcs.tctm.ccsds.UdpTmFrameLink":
            print(f"[INFO] Setting downlink port for TM link {link.get('name', '')} to {downlink_port}")
            link["port"] = downlink_port
            link["frameLength"] = constants[0]
            link["spacecraftId"] = constants[1]
            for vc in link.get("virtualChannels", []):
                vc["maxPacketLength"] = constants[0]
        elif link.get("class", "") == "org.yamcs.tctm.ccsds.UdpTcFrameLink":
            print(f"[INFO] Setting downlink port for TM link {link.get('name', '')} to {downlink_port}")
            link["port"] = uplink_port
            link["maxFrameLength"] = constants[0]
            link["spacecraftId"] = constants[1]
    with instance_path.open("w") as f:
        yaml.safe_dump(instance_config, f)

    return  yamcs_working_config_dir, fprime_instance


def launch_yamcs(parsed_args):
    """ Launch YAMCS """
    # Set up the environment variables required by YAMCS and fprime-yamcs
    environment = os.environ.copy()
    environment["FPRIME_DICTIONARY"] = parsed_args.dictionary
    environment["FPRIME_YAMCS_INSTANCE"] = parsed_args.yamcs_events_instance

    print(f"[INFO] Using FPRIME_DICTIONARY: {environment['FPRIME_DICTIONARY']}")
    print(f"[INFO] Using FPRIME_YAMCS_INSTANCE: {environment['FPRIME_YAMCS_INSTANCE']}")
    print(f"[INFO] Using YAMCS_DATA_DIR: {parsed_args.yamcs_data_dir.absolute()}")
    print(f"[INFO] Using YAMCS_CONFIG_DIR: {parsed_args.yamcs_config_dir.absolute()}")

    # Switch to the YAMCS directory and launch YAMCS using Maven
    return launch_process(
        ["mvn", "-f", str(files("fprime_yamcs").joinpath("yamcs/pom.xml")), "yamcs:run",
         f"-Dyamcs.configurationDirectory={parsed_args.yamcs_config_dir.absolute()}",
         f"-Dyamcs.directory={parsed_args.yamcs_data_dir.absolute()}"
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

        yamcs_config_dir, fprime_instance = construct_temporary_configuration(parsed_args.yamcs_config_dir, instances, parsed_args.dictionary, parsed_args.udp_uplink_port, parsed_args.udp_downlink_port)
        parsed_args.yamcs_config_dir = yamcs_config_dir
        if parsed_args.yamcs_events_instance is None:
            parsed_args.yamcs_events_instance = fprime_instance
        launched_apps = [launch_app] if parsed_args.app is not None else []
        processes = [launcher(parsed_args) for launcher in launched_apps + [launch_yamcs]]
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
