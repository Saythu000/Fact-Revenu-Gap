from pyedi import SchemaMapper
from typing import Union, List, Dict

from .mappings import MAPPINGS

class Mapper:
    """
    Maps EDI structured JSON to CSV-friendly flat dictionaries.
    
    Handles both single record (dict) and multiple records (list of dicts) from StructuredFormatter.
    """

    def __init__(self):
        # Single mapper instance for member data
        self.mapper = SchemaMapper(MAPPINGS)

    def map_member(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
          
        if isinstance(structured_json, list):
            return [self.mapper.map(record) for record in structured_json]
        
        # Handle single record
        return self.mapper.map(structured_json)
