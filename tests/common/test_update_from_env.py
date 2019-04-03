# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

import os

from unittest.mock import patch
from textwrap import dedent

from tests.base.fakemodule import FakeModule


class TestingUpdateFromEnv(TestCase):

    def test_update_from_env(self):

        fake_environ = {
            'APP_A_B_VAR_A': 'OVERRIDDEN',
            'APP_A_B_VAR_B': '2'
        }

        module_code = dedent("""
        from common.updatefromenv import update_from_env
        VAR_A = "STRING"
        VAR_B = 1
        update_from_env(__name__, 'config', 'APP_')
        """)

        with patch.object(os, 'environ', fake_environ), \
                FakeModule('config.a.b', module_code) as module:

            assert module.VAR_A == 'OVERRIDDEN'
            assert module.VAR_B == 2
