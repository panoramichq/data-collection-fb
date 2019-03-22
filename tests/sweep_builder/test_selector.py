from unittest.mock import patch, Mock

import pytest

from common.enums.failure_bucket import FailureBucket
from common.store.jobreport import JobReport
from sweep_builder.selector import should_select


@pytest.mark.parametrize('bucket', [
    FailureBucket.WorkingOnIt,
    FailureBucket.Success,
    FailureBucket.Other,
    FailureBucket.Throttling,
])
def test_should_select_last_error_not_too_large(bucket):
    report = JobReport(last_failure_bucket=bucket)

    assert should_select(report)


def test_should_select_last_error_is_too_large_none_fails_in_row():
    report = Mock(last_failure_bucket=FailureBucket.TooLarge, fails_in_row=None)

    assert should_select(report)


@patch('sweep_builder.selector.FAILS_IN_ROW_BREAKDOWN_LIMIT', new=2)
def test_should_select_last_error_is_too_large_none_fails_under_limit():
    report = Mock(last_failure_bucket=FailureBucket.TooLarge, fails_in_row=1)

    assert should_select(report)


@patch('sweep_builder.selector.FAILS_IN_ROW_BREAKDOWN_LIMIT', new=2)
def test_should_select_last_error_is_too_large_fails_in_row_over_limit():
    report = Mock(last_failure_bucket=FailureBucket.TooLarge, fails_in_row=3)

    assert not should_select(report)
