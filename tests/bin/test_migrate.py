import pytest

from unittest.mock import patch
from bin.migrate import do_it_all


def test_do_it_all_host_none():
    with patch('common.store.base.BaseMeta') as mock_base:
        mock_base.host = None
        with pytest.raises(ValueError):
            do_it_all()
