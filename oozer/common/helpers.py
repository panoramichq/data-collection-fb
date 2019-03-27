from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope


def extract_tags_for_celery_fb_task(job_scope: JobScope, _: JobContext, *__, **___):
    return {'entity_type': job_scope.entity_type, 'ad_account_id': job_scope.ad_account_id}
