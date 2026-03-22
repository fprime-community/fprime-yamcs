
""" Processor module for fprime-yamcs-events


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

import struct
from pathlib import Path
from typing import Optional

from yamcs.client import YamcsClient
from yamcs.tmtc.model import Packet

# FPrime imports
from fprime_gds.common.loaders.event_json_loader import EventJsonLoader
from fprime_gds.common.decoders.event_decoder import EventDecoder
from fprime_gds.common.utils.config_manager import ConfigManager
from .logging import logger

class FPrimeEventProcessor:
    """
    Processes FPrime event packets from YAMCS and publishes decoded events
    """
    
    # FPrime packet structure constants
    SPACE_PACKET_HEADER_LEN = 6
    FW_PACKET_DESCRIPTOR_SIZE = 2
    FW_EVENT_ID_SIZE = 4
    FW_TIME_BASE_SIZE = 2
    FW_TIME_CONTEXT_SIZE = 1
    
    # FPrime APID for events (from ComCfg.fpp)
    APID_EVENT = 2
    
    # Event data starts after: SPACE_PACKET_HEADER + FwPacketDescriptorType
    EVENT_DATA_OFFSET = SPACE_PACKET_HEADER_LEN + FW_PACKET_DESCRIPTOR_SIZE
    
    def __init__(self, yamcs_url: str, yamcs_instance: str, dictionary_path: str):
        """
        Initialize the FPrime Event Processor
        
        Args:
            yamcs_url: URL of the YAMCS server (e.g., 'http://localhost:8090')
            yamcs_instance: YAMCS instance name (e.g., 'myproject')
            dictionary_path: Path to the FPrime topology dictionary JSON file
        """
        self.yamcs_url = yamcs_url
        self.yamcs_instance = yamcs_instance
        self.dictionary_path = Path(dictionary_path)
        
        # Initialize YAMCS client
        logger.info(f"Connecting to YAMCS at {yamcs_url}, instance: {yamcs_instance}")
        self.yamcs_client = YamcsClient(yamcs_url)
        self.processor_client = self.yamcs_client.get_processor(
            instance=yamcs_instance,
            processor='realtime'
        )
        
        # Initialize FPrime event decoder
        logger.info(f"Loading FPrime dictionary from {dictionary_path}")
        self._init_fprime_decoder()
        
    def _init_fprime_decoder(self):
        """Initialize the FPrime event decoder with the topology dictionary"""
        if not self.dictionary_path.exists():
            raise FileNotFoundError(f"Dictionary not found: {self.dictionary_path}")
        
        # Load the event dictionary
        event_loader = EventJsonLoader(self.dictionary_path)
        event_dict = event_loader.get_id_dict(str(self.dictionary_path))
        
        # Create the event decoder
        self.event_decoder = EventDecoder(event_dict)
        logger.info(f"Loaded {len(event_dict)} event definitions")
        
    def _extract_event_data(self, packet_data: bytes) -> Optional[bytes]:
        """
        Extract the event data portion from a CCSDS packet
        
        Args:
            packet_data: Raw packet bytes
            
        Returns:
            Event data bytes (starting with event ID) or None if invalid
        """
        if len(packet_data) < self.EVENT_DATA_OFFSET:
            logger.warning(f"Packet too short: {len(packet_data)} bytes")
            return None
        
        # Extract event data (everything after SPACE_PACKET_HEADER + FwPacketDescriptorType)
        # This includes: FwEventIdType + TimeTag + Event Arguments
        event_data = packet_data[self.EVENT_DATA_OFFSET:]
        
        return event_data
    
    def _process_event_packet(self, tm_packet: Packet):
        """
        Process a single FPrime event packet
        
        Args:
            tm_packet: YAMCS Packet object
        """
        try:
            packet_data = tm_packet.binary
            
            # Verify APID
            apid_seqcount = struct.unpack('>I', packet_data[0:4])[0]
            apid = (apid_seqcount >> 16) & 0x07FF
            
            if apid != self.APID_EVENT:
                logger.debug(f"Skipping non-event packet with APID {apid}")
                return
            
            # Extract event data
            event_data = self._extract_event_data(packet_data)
            if event_data is None:
                return
            
            # Decode using FPrime decoder
            event_list = self.event_decoder.decode_api(event_data)
            
            if not event_list:
                logger.warning("No events decoded from packet")
                return
            
            # Process each decoded event (usually one per packet)
            for event in event_list:
                self._publish_event(event, tm_packet.generation_time)
                
        except Exception as e:
            logger.error(f"Error processing event packet: {e}", exc_info=True)
    
    def _publish_event(self, event_data, generation_time):
        """
        Publish a decoded FPrime event to YAMCS
        
        Args:
            event_data: Decoded EventData object from FPrime
            generation_time: Packet generation time from YAMCS
        """
        try:
            # Get event metadata
            event_name = event_data.template.get_name()
            event_id = event_data.id
            severity = event_data.get_severity()
            display_text = event_data.get_display_text()
            
            # Map FPrime severity to YAMCS severity
            # FPrime severities: COMMAND, ACTIVITY_LO, ACTIVITY_HI, WARNING_LO, 
            #                    WARNING_HI, DIAGNOSTIC, FATAL
            severity_mapping = {
                'COMMAND': 'info',
                'ACTIVITY_LO': 'info',
                'ACTIVITY_HI': 'watch',
                'WARNING_LO': 'warning',
                'WARNING_HI': 'distress',
                'DIAGNOSTIC': 'info',
                'FATAL': 'critical',
                'SEVERITY_COMMAND': 'info',
                'SEVERITY_ACTIVITY_LO': 'info',
                'SEVERITY_ACTIVITY_HI': 'watch',
                'SEVERITY_WARNING_LO': 'warning',
                'SEVERITY_WARNING_HI': 'distress',
                'SEVERITY_DIAGNOSTIC': 'info',
                'SEVERITY_FATAL': 'critical',
            }
            
            severity_str = str(severity).upper()
            yamcs_severity = severity_mapping.get(severity_str, 'info')
            
            # Build event message
            message = f"[{event_name}] {display_text}"
            
            # Get event arguments as key-value pairs
            event_args = {}
            if event_data.args:
                arg_templates = event_data.template.get_args()
                for idx, arg in enumerate(event_data.args):
                    arg_name, _, _ = arg_templates[idx]
                    event_args[arg_name] = str(arg.val)
            
            # Create YAMCS event
            logger.info(f"Publishing event: {event_name} (ID: {event_id}, Severity: {yamcs_severity})")
            logger.info(f"  Message: {display_text}")
            if event_args:
                logger.info(f"  Arguments: {event_args}")
            
            # Publish to YAMCS
            # Note: The yamcs-client Python library may not support direct event publishing
            # In that case, you might need to use the REST API directly
            # For now, we'll log the event and show how it would be published
            
            # Option 1: Using REST API (if available)
            self.yamcs_client.send_event(
                instance=self.yamcs_instance,
                source='FPrimeEventProcessor',
                event_type=event_name,
                severity=yamcs_severity,
                message=message,
            )
            
            # Option 2: Create a parameter update (alternative approach)
            # For demonstration, we'll just log it
            logger.info(f"✓ Event decoded and ready for publishing: {event_name}")
            
        except Exception as e:
            logger.error(f"Error publishing event: {e}", exc_info=True)
    
    def start(self):
        """
        Start the event processor and subscribe to YAMCS packet stream
        """
        logger.info("Starting FPrime Event Processor")
        logger.info(f"Subscribing to tm_realtime stream, filtering APID {self.APID_EVENT}")
        
        try:
            # Subscribe to TM packets
            subscription = self.processor_client.create_packet_subscription(
                on_data=self._process_event_packet,
            )
            
            logger.info("Event processor running. Press Ctrl+C to stop.")
            
            # Keep the script running
            while True:
                pass
            
        except KeyboardInterrupt:
            logger.info("Shutting down event processor")
        except Exception as e:
            logger.error(f"Error in event processor: {e}", exc_info=True)
            raise


