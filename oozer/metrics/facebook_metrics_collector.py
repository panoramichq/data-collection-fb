from oozer.common.facebook_collector import FacebookCollector
from oozer.common.facebook_async_report import FacebookAsyncReport
from oozer.common.enum import FB_ADACCOUNT_MODEL


class FacebookMetricsCollector(FacebookCollector):

    def get_insights(self, edge_entity, entity_id, report_params):
        """

        :param edge_entity:
        :param entity_id:
        :param report_params:
        :return FacebookAsyncReport:
        """
        if edge_entity is FB_ADACCOUNT_MODEL:
            edge_instance = self._get_ad_account(entity_id)
        else:
            edge_instance = edge_entity(fbid=entity_id)

        result = edge_instance.get_insights_async(params=report_params)

        return FacebookAsyncReport(result['id'], self.token)


def collect_insights(job_scope):
    """
    :param JobScope job_scope: The JobScope as we get it from the task itself
    """
    pass
