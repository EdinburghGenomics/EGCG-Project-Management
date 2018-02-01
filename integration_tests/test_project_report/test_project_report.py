import os
from unittest.mock import patch
from integration_tests import IntegrationTest
from egcg_core import rest_communication
from egcg_core.config import cfg
from project_report import client
from tests.test_project_report import FakeLims, fake_rest_api_samples
work_dir = os.path.dirname(__file__)


class TestProjectReport(IntegrationTest):
    delivery_source = os.path.join(work_dir, 'delivery_source')
    delivery_dest = os.path.join(work_dir, 'delivery_dest')
    patches = (
        patch('project_report.connection', return_value=FakeLims()),
    )

    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.delivery_dest, exist_ok=True)
        cfg.content = {
            'sample': {
                'delivery_source': cls.delivery_source,
                'delivery_dest': cls.delivery_dest
            }
        }

    def setUp(self):
        super().setUp()
        for s in fake_rest_api_samples:
            rest_communication.post_entry('samples', s)

    def test_reports(self):
        for p in ('htn999', 'nhtn999', 'hpf999', 'nhpf999', 'uhtn999'):
            client.main(['-p', p, '-o', 'html', '-w', work_dir])
