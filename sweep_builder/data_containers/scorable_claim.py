from common.job_signature import JobSignature
from common.store.jobreport import JobReport
from sweep_builder.data_containers.expectation_claim import ExpectationClaim


class ScorableClaim(ExpectationClaim):
    """Expectation claim ready for scoring."""

    selected_job_signature: JobSignature = None
    last_report: JobReport = None

    @property
    def selected_job_id(self):
        return self.selected_job_signature.job_id
