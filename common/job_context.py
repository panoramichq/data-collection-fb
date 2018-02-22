from typing import Tuple, Dict


class JobContext:

    entity_checksums = None  # type: Dict[str, Tuple[str, str]]
    """
    Checksums are in this format: 
    
    {
        '<entity_id'>: (entity_hash, fields_hash),
    }
    """

    def __init__(self):
        self.entity_checksums = dict()

