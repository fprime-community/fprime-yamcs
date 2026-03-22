""" Logging functionality to share across the fprime_yamcs_events package

This module sets up a logger for the fprime_yamcs_events package that can be imported and used by all modules in the
package. This allows for consistent logging configuration.
"""
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')
logger = logging.getLogger("fprime_yamcs_events")