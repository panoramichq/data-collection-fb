from datetime import datetime

from common.enums.entity import Entity
from oozer.common.cold_storage.base_store import _job_scope_to_metadata
from oozer.common.job_scope import JobScope


def test__job_scope_to_metadata():
    scope = JobScope(
        job_id='job identifier',
        namespace='fb',
        ad_account_id='007',
        report_type='report type',
        entity_type=Entity.Campaign,
        range_start=datetime.fromtimestamp(1),
        score=10,
    )

    result = _job_scope_to_metadata(scope)

    result.pop('extracted_at')
    result.pop('build_id')

    assert {
        'job_id': 'fb|007|C||report+type||1970-01-01T00%3A00%3A01',
        'ad_account_id': '007',
        'report_type': 'report type',
        'entity_type': 'C',
        'platform_api_version': 'v3.1',
        'platform': 'fb',
        'score': 10,
    } == result

