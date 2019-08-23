from unittest.mock import patch

from bin.disk_space_usage_analyser import DiskSpaceUsageAnalysisHelper
from disk_space_usage_analysis import RunDirectoryChecker
from egcg_core.config import cfg
from tests import TestProjectManagement


class TestRunDirectoryChecker(TestProjectManagement):
    """This test suite checks the functionality of the run directory checker."""
    def setUp(self):
        cfg.content.update({
                            'directory_space_analysis':
                            {
                                 'runs_dir': '/lustre/edgeprod/processed/runs/',
                                 'projects_dir': '/lustre/edgeprod/processed/projects/',
                                 'output_dir': '~/Documents/development/output/'
                            }
                         })
        self.disk_usage_helper = DiskSpaceUsageAnalysisHelper()
        self.response = [
            {
                'sample_id': 'LP1251554__B_21',
                'data_deleted': 'all'
            },
            {
                'sample_id': '10015AT0022',
                'data_deleted': 'none'
            },
            {
                'sample_id': 'X0002DM002_D_07',
                'data_deleted': 'none'
            }
        ]

    def test_debug_logging_level_true(self):
        with patch('egcg_core.rest_communication.get_documents', return_value=self.response):
            RunDirectoryChecker(self.disk_usage_helper).main()

    def tearDown(self) -> None:
        """Remove files created during testing process"""
        pass
