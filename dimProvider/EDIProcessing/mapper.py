from pyedi import SchemaMapper
from typing import Union, List, Dict

from .mappings import MAPPINGS

class Mapper:
    """
    Maps EDI structured JSON to CSV-friendly flat dictionaries.
    
    Handles both single record (dict) and multiple records (list of dicts) from StructuredFormatter.
    Matches the colleague's generic mapping architecture.
    """

    def __init__(self):
        self.mapper = SchemaMapper(MAPPINGS)

    def map_provider(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        if isinstance(structured_json, list):
            return [self.mapper.map(record) for record in structured_json]
        
        # Handle single record
        return self.mapper.map(structured_json)
