from unittest.mock import patch

from bin.disk_space_usage_analyser import DiskSpaceUsageAnalysisHelper
from disk_space_usage_analysis import RunsDirectoryChecker
from egcg_core.config import cfg
from tests import TestProjectManagement


class TestRunDirectoryChecker(TestProjectManagement):
    """This test suite checks the functionality of the run directory checker."""
    def setUp(self):
        self.disk_usage_dir = os.path.join(self.assets_path,  'disk_space_usage')
        runs_dir = os.path.join(self.disk_usage_dir, 'runs')
        projects_dir = os.path.join(self.disk_usage_dir, 'projects')
        output_dir = os.path.join(self.disk_usage_dir, 'output')
        os.makedirs(runs_dir, exist_ok=True)
        os.makedirs(projects_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        cfg.content['directory_space_analysis'] = {}
        cfg.content['directory_space_analysis']['runs_dir'] = runs_dir
        cfg.content['directory_space_analysis']['projects_dir'] = projects_dir
        cfg.content['directory_space_analysis']['output_dir'] = output_dir

        self.disk_usage_helper = DiskSpaceUsageAnalysisHelper()
        self.response = [
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
        os.makedirs(os.path.join(runs_dir, 'project1', 'sample1'), exist_ok=True)
        touch(os.path.join(runs_dir, 'project1', 'sample1', 'sample1_R1.fastq.gz'), content='A fastq file')
        touch(os.path.join(runs_dir, 'project1', 'sample1', 'sample1_R2.fastq.gz'), content='A fastq file')

    def setUp(self):
        self.disk_usage_helper = DiskSpaceUsageAnalysisHelper()

    def test_run_directory_checker(self):
        with patch('egcg_core.rest_communication.get_documents', return_value=self.response):
            RunsDirectoryChecker(self.disk_usage_helper).main()

    @classmethod
    def tearDownClass(cls):
        """Remove files created during testing process"""
        shutil.rmtree(self.disk_usage_dir)
