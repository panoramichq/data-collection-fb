from typing import NewType, Callable, Generator

from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim

ExpectationGeneratorType = NewType(
    'ExpectationGeneratorType', Callable[[RealityClaim], Generator[ExpectationClaim, None, None]]
)
