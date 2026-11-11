""" Main entrypoint for F Prime YAMCS TlmChan processing

This is the main function for running fprime-yamcs-tlmchan, which splits aggregate F Prime
telemetry channel packets into individual channel packets for YAMCS.

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
import argparse
import logging
import os

from .processor import FPrimeTlmChanProcessor
from .logging import logger

def parse_args():
    """ Parse arguments for the FPrime YAMCS TlmChan Processor
    """
    parser = argparse.ArgumentParser(description='FPrime TlmChan Processor for YAMCS')
    parser.add_argument('--yamcs-url',
        default='http://localhost:8090',
        help='YAMCS server URL (default: http://localhost:8090)'
    )
    parser.add_argument(
        '--instance',
        default=os.environ.get('FPRIME_YAMCS_INSTANCE', 'fprime-project'),
        help='YAMCS instance name (default: fprime-project)'
    )

    parser.add_argument(
        '--dictionary',
        help='Path to FPrime topology dictionary JSON file',
        default=os.environ.get('FPRIME_DICTIONARY', None)
    )

    parser.add_argument(
        '--inject-host',
        default=os.environ.get('FPRIME_YAMCS_TM_INJECT_HOST', 'localhost'),
        help='Host to send split single-record telemetry packets to (default: localhost)'
    )

    parser.add_argument(
        '--inject-port',
        type=int,
        default=int(os.environ.get('FPRIME_YAMCS_TM_INJECT_PORT', '50002')),
        help='UDP port of the YAMCS telemetry packet link for split packets (default: 50002)'
    )

    parser.add_argument(
        '--space-system',
        default=os.environ.get('FPRIME_YAMCS_SPACE_SYSTEM', None),
        help='XTCE root space system of this deployment in the MDB (default: derived from the dictionary)'
    )
    parser.add_argument(
       '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()
    if args.dictionary is None:
        parser.error("Supply --dictionary or set the FPRIME_DICTIONARY environment variable")

    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    return args


def main():
    """Main entry point for the TlmChan processor"""
    args = parse_args()

    # Create and start processor
    try:
        processor = FPrimeTlmChanProcessor(
            yamcs_url=args.yamcs_url,
            yamcs_instance=args.instance,
            dictionary_path=str(args.dictionary),
            inject_host=args.inject_host,
            inject_port=args.inject_port,
            space_system=args.space_system,
        )
        processor.start()
    except Exception as e:
        logger.error(f"Failed to start processor: {e}")
        raise


if __name__ == '__main__':
    main()
