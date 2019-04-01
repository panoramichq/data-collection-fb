from typing import Tuple, Dict, List


class JobContext:
    """
    A context style class that is used to hold two types of information provided
    to individual worker tasks:

    - existing entity hashes
    - information about which normative tasks are "containted" in the effective
        task that is being executed

    """

    entity_checksums: Dict[str, Tuple[str, str]] = None
    """
    Checksums are in this format:

    {
        '<entity_id'>: (entity_hash, fields_hash),
    }
    """

    normative_tasks: List[str] = None
    """
    A simple list of "normative" tasks being represented by the effective task
    being executed. The representation as a job_id identifier is sufficient and
    can be parsed out if needed:

    [

    ]
    """

    def __init__(self):
        self.entity_checksums = {}
        self.normative_tasks = []
