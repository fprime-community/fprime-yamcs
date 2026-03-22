""" Main entrypoint for F Prime YAMCS Event processing
"""
import argparse
import logging
import os
import sys

from .processor import FPrimeEventProcessor
from .logging import logger

def parse_args():
    """ Parse arguments for the FPrime YAMCS Event Processor
    """
    parser = argparse.ArgumentParser(description='FPrime Event Processor for YAMCS')
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
    """Main entry point for the event processor"""
    args = parse_args()
    
    # Create and start processor
    try:
        processor = FPrimeEventProcessor(
            yamcs_url=args.yamcs_url,
            yamcs_instance=args.instance,
            dictionary_path=str(args.dictionary)
        )
        processor.start()
    except Exception as e:
        logger.error(f"Failed to start processor: {e}")
        raise
        sys.exit(1)


if __name__ == '__main__':
    main()
