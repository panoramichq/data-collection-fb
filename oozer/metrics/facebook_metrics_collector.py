import time
from oozer.common.facebook_collector import FacebookCollector
from oozer.common.facebook_async_report import FacebookAsyncReport
from oozer.common.enum import FB_ADACCOUNT_MODEL


class FacebookMetricsCollector(FacebookCollector):
    DEFAULT_POLLING_INTERVAL = 1

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
            edge_instance = edge_entity(fbid=entity_id, api=self.api)

        result = edge_instance.get_insights(params=report_params, async=True)

        # TODO: move this to .collect_insights to allow reporting on report stages
        report_wrapper = FacebookAsyncReport(result['id'], self.token)
        while not report_wrapper.completed():
            report_wrapper.refresh()
            time.sleep(self.DEFAULT_POLLING_INTERVAL)

        return report_wrapper.read()



def collect_insights(job_scope):
    """
    :param JobScope job_scope: The JobScope as we get it from the task itself
    """
    pass
