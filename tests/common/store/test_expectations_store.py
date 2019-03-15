# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from tests.base import random

from oozer.common.expecations_store import JobExpectationsWriter, iter_expectations


class JobExpectationsStoreTests(TestCase):
    def test_expectations_store(self):

        all_job_ids_should_be = set()
        job_id_template = 'fb|{ad_account_id}|{job_variant}'.format

        sweep_id = random.gen_string_id()
        ad_account_ids = ['1', '2', '3']
        job_variants = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l']

        with JobExpectationsWriter(sweep_id=sweep_id) as add_expectation:
            for ad_account_id in ad_account_ids:
                for job_variant in job_variants:
                    job_id = job_id_template(ad_account_id=ad_account_id, job_variant=job_variant)

                    all_job_ids_should_be.add(job_id)

                    add_expectation(
                        job_id,
                        ad_account_id,
                        None  # we don't care about entity ID at this time.
                    )

        all_job_ids_actual = list(iter_expectations(sweep_id))

        # must test as list to ensure that duplicate entries are not returned
        # once collapsed into a set() dupes disappear and it's too late to test
        assert len(all_job_ids_actual) == len(all_job_ids_should_be), "Must have no dupes returned"

        # now let's test for equality
        assert set(all_job_ids_actual) == all_job_ids_should_be
