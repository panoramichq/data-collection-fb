from common.enums.entity import Entity
from common.job_signature import JobSignature
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.pipeline import iter_dedup_expectations


def test_iter_dedup_expectations():
    expectations = [
        ExpectationClaim(entity_id=1, entity_type=Entity.Ad, job_signatures=[JobSignature.bind('job1')]),
        ExpectationClaim(entity_id=2, entity_type=Entity.Ad, job_signatures=[JobSignature.bind('job2')]),
        ExpectationClaim(entity_id=1, entity_type=Entity.Ad, job_signatures=[JobSignature.bind('job1')]),
    ]

    result = list(iter_dedup_expectations(expectations))

    assert ['job1', 'job2'] == [e.job_signatures[-1].job_id for e in result]
