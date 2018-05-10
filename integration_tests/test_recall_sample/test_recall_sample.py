import os
import shutil
from time import sleep
from egcg_core import archive_management, rest_communication
from egcg_core.config import cfg
from egcg_core.exceptions import EGCGError
from unittest.mock import patch
import integration_tests
import data_deletion.client
from bin import recall_sample


class TestRecall(integration_tests.IntegrationTest):
    def setUp(self):
        super().setUp()

        self.fastq_dir = os.path.join(self.run_dir, 'fastqs')
        self.processed_data_dir = os.path.join(self.run_dir, 'processed_data')
        self.delivered_data_dir = os.path.join(self.run_dir, 'delivered_data')
        self.fastq_archive_dir = os.path.join(self.run_dir, 'fastq_archives')
        self.processed_archive_dir = os.path.join(self.run_dir, 'processed_archives')

        self.all_files = integration_tests.setup_delivered_samples(self.processed_data_dir, self.delivered_data_dir, self.fastq_dir)
        cfg.content = {
            'executor': self.cfg['executor'],
            'data_deletion': {
                'fastqs': self.fastq_dir,
                'fastq_archives': self.fastq_archive_dir,
                'processed_data': self.processed_data_dir,
                'processed_archives': self.processed_archive_dir,
                'delivered_data': self.delivered_data_dir
            }
        }

    def assert_hsm_state_for_sample(self, sample_id, state_func, retries=10):
        files = self.all_files[sample_id]
        exp = {f: True for f in files}
        for r in range(retries):
            if {f: state_func(f) for f in files} == exp:
                break
            else:
                sleep(30)

        self.assertEqual('%s %s' % (sample_id, state_func.__name__), {f: state_func(f) for f in files}, exp)

    def assert_api_state_for_sample(self, sample_id, exp_state):
        self.assertEqual('%s api state', rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'], exp_state)

    def setup_samples_for_recall(self):
        with patch('data_deletion.client.load_config'):
            data_deletion.client.main(['delivered_data', '--manual_delete', 'sample_1', 'sample_2', '--work_dir', self.run_dir])
            for s in ('sample_1', 'sample_2'):
                self.assert_api_state_for_sample(s, 'on lustre')
                self.assert_hsm_state_for_sample(s, archive_management.is_released)

            self.assert_api_state_for_sample('sample_3', 'none')
            self.assert_hsm_state_for_sample('sample_3', archive_management.is_archived)

    def test_recall(self):
        self.setup_samples_for_recall()

        with patch('bin.recall_sample.load_config'):
            recall_sample.main(['restore', 'sample_1'])
            self.assert_api_state_for_sample('sample_1', 'none')
            self.assert_api_state_for_sample('sample_2', 'on lustre')
            for sample_id in ('sample_1', 'sample_2', 'sample_3'):
                self.assert_hsm_state_for_sample(sample_id, archive_management.is_archived)

    def test_dodgy_recall(self):
        self.setup_samples_for_recall()

        # unarchive a file
        file_to_unarchive = self.all_files['sample_1'][0]
        shutil.copy(file_to_unarchive, file_to_unarchive + '.tmp')
        os.remove(file_to_unarchive)
        shutil.move(file_to_unarchive + '.tmp', file_to_unarchive)

        with patch('bin.recall_sample.load_config'):
            with self.assertRaises(EGCGError) as e:
                recall_sample.main(['restore', 'sample_1'])
                self.assertEqual('exception content', e.exception.args, ('Found 0 dirty, 1 unarchived files',))

        self.assert_api_state_for_sample('sample_1', 'on lustre')  # nothing should have happened
