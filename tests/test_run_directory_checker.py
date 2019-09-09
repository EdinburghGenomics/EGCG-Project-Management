import os
from unittest.mock import patch

from bin.disk_space_usage_analyser import DiskSpaceUsageAnalysisHelper
from disk_space_usage_analysis import RunsDirectoryChecker
from egcg_core.config import cfg
from tests import TestProjectManagement


class TestRunDirectoryChecker(TestProjectManagement):
    """This test suite checks the functionality of the run directory checker."""
    # TODO: Change directories prior to final commit
    @classmethod
    def setUpClass(cls):
        print('Setting up class')
        cfg.content.update({
                            'directory_space_analysis':
                            {
                                 'runs_dir': '/lustre/edgeprod/processed/runs/',
                                 'projects_dir': '/lustre/edgeprod/processed/projects/',
                                 'output_dir': '/Users/lbuttigi/Documents/development/output/'
                            }
                         })

        cls.response = [
            {
                'sample_id': 'LP1251554__B_11',
                'data_deleted': 'all'
            },
            {
                'sample_id': '10015AT0004',
                'data_deleted': 'none'
            },
            {
                'sample_id': 'X0002DM002_D_07',
                'data_deleted': 'none'
            }
        ]

    def setUp(self):
        self.disk_usage_helper = DiskSpaceUsageAnalysisHelper()

    def test_run_directory_checker(self):
        with patch('egcg_core.rest_communication.get_documents', return_value=self.response):
            RunsDirectoryChecker(self.disk_usage_helper).main()

    @classmethod
    def tearDownClass(cls):
        """Remove files created during testing process"""
        for file in ['residual_run_dir_analysis.log', 'residual_run_directory_analysis.csv', 'run_dir_analysis.csv',
                     'run_dir_analysis.log']:
            os.remove(cfg.query('directory_space_analysis')['output_dir'] + file)
