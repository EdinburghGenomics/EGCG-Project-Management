import os
import shutil
from time import sleep
from collections import defaultdict
from egcg_core import archive_management, rest_communication
from egcg_core.config import cfg
from egcg_core.exceptions import EGCGError
from unittest.mock import patch
from integration_tests import IntegrationTest, integration_cfg
import data_deletion.client
from bin import recall_sample

work_dir = os.path.dirname(__file__)


class TestRecall(IntegrationTest):
    fastq_dir = os.path.join(work_dir, 'fastqs')
    processed_data_dir = os.path.join(work_dir, 'processed_data')
    delivered_data_dir = os.path.join(work_dir, 'delivered_data')
    fastq_archive_dir = os.path.join(work_dir, 'fastq_archives')
    processed_archive_dir = os.path.join(work_dir, 'processed_archives')

    @classmethod
    def setUpClass(cls):
        cfg.content = {
            'executor': integration_cfg['executor'],
            'data_deletion': {
                'fastqs': cls.fastq_dir,
                'fastq_archives': cls.fastq_archive_dir,
                'processed_data': cls.processed_data_dir,
                'processed_archives': cls.processed_archive_dir,
                'delivered_data': cls.delivered_data_dir
            }
        }

    def setUp(self):
        super().setUp()

        for d in (self.processed_data_dir, self.delivered_data_dir, self.fastq_dir):
            if os.path.isdir(d):
                shutil.rmtree(d)

        self.all_files = defaultdict(list)
        for i in range(1, 4):
            sample_id = 'sample_' + str(i)
            ext_sample_id = 'ext_' + sample_id
            sample_dir = os.path.join(self.processed_data_dir, 'a_project', sample_id)
            delivered_dir = os.path.join(self.delivered_data_dir, 'a_project', 'a_delivery_date', sample_id)
            fastq_dir = os.path.join(self.fastq_dir, 'a_run', 'a_project', sample_id)

            os.makedirs(sample_dir)
            os.makedirs(fastq_dir)
            os.makedirs(delivered_dir)

            rest_communication.post_entry(
                'samples',
                {'sample_id': sample_id, 'user_sample_id': ext_sample_id, 'project_id': 'a_project'}
            )
            rest_communication.post_entry(
                'run_elements',
                {'run_element_id': 'a_run_%s_ATGC' % i, 'run_id': 'a_run', 'lane': i, 'barcode': 'ATGC',
                 'project_id': 'a_project', 'sample_id': sample_id, 'library_id': 'a_library'}
            )

            for ext in ('.bam', '.vcf.gz'):
                f = os.path.join(sample_dir, ext_sample_id + ext)
                self.all_files[sample_id].append(f)

            for r in ('1', '2'):
                f = os.path.join(fastq_dir, 'L00%s_R%s.fastq.gz' % (i, r))
                self.all_files[sample_id].append(f)

            for f in self.all_files[sample_id]:
                open(f, 'w').close()
                os.link(f, os.path.join(delivered_dir, os.path.basename(f)))
                archive_management.register_for_archiving(f)

        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            for f in self.all_files[sample_id]:
                while not archive_management.is_archived(f):
                    sleep(10)

    def assert_hsm_state_for_sample(self, sample_id, state_func, retries=10):
        if not all(state_func(f) for f in self.all_files[sample_id]):
            if retries > 0:
                sleep(30)
                self.assert_hsm_state_for_sample(sample_id, state_func, retries - 1)
            else:
                raise AssertionError(
                    'Timed out waiting for all files to be %s: %s' % (
                        state_func, {f: archive_management.archive_states(f) for f in self.all_files[sample_id]}
                    )
                )

    @staticmethod
    def assert_api_state_for_sample(sample_id, exp_state):
        assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == exp_state

    def setup_samples_for_recall(self):
        with patch('data_deletion.client.load_config'):
            data_deletion.client.main(['delivered_data', '--manual_delete', 'sample_1', 'sample_2', '--work_dir', work_dir])
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
                assert e.exception.args == ('Found 0 dirty, 1 unarchived files',)

        self.assert_api_state_for_sample('sample_1', 'on lustre')  # nothing should have happened
