# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id
from common.job_signature import JobSignature
from oozer.common.job_scope import JobScope
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from oozer.looper import iter_tasks
from sweep_builder import persister
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim
from tests.base import random


class PersisterSavesJobScopeData(TestCase):

    def test_persister_saves_job_scope_auxiliary_data_to_data_flower(self):
        # There is a need to save some context data that does not fit on JobIS
        # Persister should store that on the Data Flower.

        sweep_id = random.gen_string_id()
        entity_id = random.gen_string_id()
        ad_account_id = random.gen_string_id()

        job_id = generate_id(
            ad_account_id=ad_account_id,
            report_type=ReportType.lifetime,
            report_variant=Entity.Campaign,
        )

        prioritized_iter = [
            PrioritizationClaim(
                entity_id=entity_id,
                entity_type=Entity.Campaign,
                ad_account_id=ad_account_id,
                token='blah',
                timezone='Europe/London',
                job_signatures=[
                    # at this point persister deals with forming the
                    # auxiliary data blob for saving on Data Flower.
                    # We don't have to do that there.
                    # It can be pre-computed and placed on the JobSignature
                    JobSignature.bind(
                        job_id
                        # Here
                        # As it takes args, kwargs
                        # that we can package into Data Flower
                    )
                    # TODO: contemplate moving auxiliary data formation to
                    #       place where JobSignatures are generated and use that
                    #       data for Data Flower (as it was originally intended
                    #       but not implemented because saving each job's data
                    #       individually to Data Flower was too slow)
                ],
                job_scores=[100]
            )
        ]

        persisted = persister.iter_persist_prioritized(
            sweep_id,
            prioritized_iter
        )
        cnt = 0
        for item in persisted:
            cnt += 1
            # just need to spin the generator
            # so it does all the saving it needs to do per item
        assert cnt == 1

        # Now, finally, the testing:

        jobs_queued_actual = []
        with SortedJobsQueue(sweep_id).JobsReader() as jobs_iter:
            for job_id, job_scope_data, score in jobs_iter:
                jobs_queued_actual.append(
                    (job_id, job_scope_data, score)
                )

        jobs_queued_should_be = [
            (
                job_id,
                # Contents of this dict is what we are testing here
                dict(
                    # comes from Persister code
                    # manually peeled off *Claim and injected into Data Flower
                    ad_account_timezone_name='Europe/London',
                ),
                100
            ),
        ]

        assert jobs_queued_actual == jobs_queued_should_be

        # And, another way of looking at it
        # looper.iter_tasks preassembles JobScope and should apply aux data to it.

        job_scope = None
        cnt = 0
        for celery_task, job_scope, job_context, score in iter_tasks(sweep_id):
            cnt += 1
            # this just needs to spin once
        assert cnt == 1

        job_scope_should_be = JobScope(
            sweep_id=sweep_id,
            namespace='fb',
            ad_account_id=ad_account_id,
            report_type=ReportType.lifetime,
            report_variant=Entity.Campaign,
            # \/ This is what we are testing \/
            # comes from Persister code
            # manually peeled off *Claim and injected into Data Flower
            ad_account_timezone_name='Europe/London',
        )

        assert job_scope.to_dict() == job_scope_should_be.to_dict()
