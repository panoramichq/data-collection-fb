from unittest.mock import patch, Mock

from common.enums.entity import Entity
from oozer.common.job_scope import JobScope
from oozer.set_inaccessible_entity_task import set_inaccessible_entity_task


def test_set_inaccessible_entity_task():
    mock_model = Mock()
    mock_factory = Mock(return_value=mock_model)
    with patch.dict('oozer.set_inaccessible_entity_task.ENTITY_TYPE_MODEL_MAP', {Entity.PagePost: mock_factory}):
        set_inaccessible_entity_task(JobScope(report_variant=Entity.PagePost))

        mock_model.update.assert_called_once_with(actions=[mock_factory.is_accessible.set(False)])
