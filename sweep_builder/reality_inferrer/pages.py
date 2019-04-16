from typing import Generator

from common.store import entities


def iter_active_pages_per_scope(scope: str) -> Generator[entities.PageEntity, None, None]:
    """
    :param scope: The PageScope id
    :return: A generator of Page IDs for Pages marked "active" in our system
    """
    for page_record in entities.PageEntity.query(scope):
        if page_record.is_active and page_record.is_accessible:
            yield page_record
