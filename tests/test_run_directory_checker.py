import os
import shutil
from unittest.mock import patch

from bin.disk_space_usage_analyser import DiskSpaceUsageAnalysisHelper
from disk_space_usage_analysis import RunsDirectoryChecker
from egcg_core.config import cfg
from tests import TestProjectManagement
from tests.test_data_delivery import touch


class TestRunDirectoryChecker(TestProjectManagement):
    """This test suite checks the functionality of the run directory checker."""
    @classmethod
    def setUpClass(cls):
        cls.disk_usage_dir = os.path.join(cls.assets_path, 'disk_space_usage')
        # Making dirs
        runs_dir = os.path.join(cls.disk_usage_dir, 'runs')
        projects_dir = os.path.join(cls.disk_usage_dir, 'projects')
        output_dir = os.path.join(cls.disk_usage_dir, 'output')
        os.makedirs(runs_dir, exist_ok=True)
        os.makedirs(projects_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        # Populating cfg
        cfg.content['directory_space_analysis'] = {}
        cfg.content['directory_space_analysis']['runs_dir'] = runs_dir
        cfg.content['directory_space_analysis']['projects_dir'] = projects_dir
        cfg.content['directory_space_analysis']['output_dir'] = output_dir

        cls.disk_usage_helper = DiskSpaceUsageAnalysisHelper()

        cls.response = [
            {
                'sample_id': 'sample1',
                'data_deleted': 'all'
            },
            {
                'sample_id': 'sample2',
                'data_deleted': 'none'
            },
            {
                'sample_id': 'sample3',
                'data_deleted': 'none'
            }
        ]

        # Generating project files
        os.makedirs(os.path.join(runs_dir, 'project1', 'sample1'), exist_ok=True)
        touch(os.path.join(runs_dir, 'project1', 'sample1', 'sample1_R1.fastq.gz'), content='A fastq file')
        touch(os.path.join(runs_dir, 'project1', 'sample1', 'sample1_R2.fastq.gz'), content='A fastq file')

        os.makedirs(os.path.join(runs_dir, 'project1', 'sample2'), exist_ok=True)
        touch(os.path.join(runs_dir, 'project1', 'sample1', 'sample2_R1.fastq.gz'), content='A fastq file')
        touch(os.path.join(runs_dir, 'project1', 'sample1', 'sample2_R2.fastq.gz'), content='A fastq file')

        os.makedirs(os.path.join(runs_dir, 'project1', 'sample3'), exist_ok=True)
        touch(os.path.join(runs_dir, 'project1', 'sample3', 'sample1_R1.fastq.gz'), content='A fastq file')
        touch(os.path.join(runs_dir, 'project1', 'sample3', 'sample1_R2.fastq.gz'), content='A fastq file')

    def setUp(self):
        self.disk_usage_helper = DiskSpaceUsageAnalysisHelper()

    def test_run_directory_checker(self):
        with patch('egcg_core.rest_communication.get_documents', return_value=self.response):
            RunsDirectoryChecker(self.disk_usage_helper).main()

    @classmethod
    def tearDownClass(cls):
        """Remove files created during testing process"""
        shutil.rmtree(cls.disk_usage_dir)
