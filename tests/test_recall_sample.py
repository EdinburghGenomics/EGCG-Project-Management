from os.path import join
from unittest.mock import patch
from bin import recall_sample
from tests import TestProjectManagement
from egcg_core.exceptions import EGCGError
import logging


def fake_find_file(*parts):
    return join(*parts)


class TestRecall(TestProjectManagement):
    fake_file_states = {
        'this.vcf.gz': ['exists', 'archived', 'released'],
        'this.bam': [],
        'this_r1.fastq.gz': ['exists', 'dirty', 'archived'],
        'this_r2.fastq.gz': ['exists', 'archived']
    }

    @classmethod
    def setUpClass(cls):
        recall_sample.cfg.load_config_file(join(cls.root_path, 'etc', 'example_data_deletion.yaml'))

    @patch('bin.recall_sample.file_states', return_value=fake_file_states)
    @patch('bin.recall_sample.get_file_list_size', return_value=1000000000)
    @patch('egcg_core.archive_management.archive_states', return_value=[])
    def test_check(self, mocked_archive_states, mocked_file_size, mocked_file_states):
        assert recall_sample.check('a_sample_id') == (
            ['this.vcf.gz'],
            ['this.bam', 'this_r2.fastq.gz'],
            ['this_r1.fastq.gz']
        )

    @patch('bin.recall_sample.logger._log')
    @patch('bin.recall_sample.rest_communication.patch_entry')
    @patch('bin.recall_sample.am.recall_from_tape')
    @patch('bin.recall_sample.disk_usage')
    @patch('bin.recall_sample.check')
    def test_restore(self, mocked_check, mocked_disk_usage, mocked_recall, mocked_patch, mocked_log):
        mocked_disk_usage.return_value.free = 1
        with self.assertRaises(EGCGError) as e:
            recall_sample.restore('a_sample_id')

        assert str(e.exception) == 'Unsafe to recall: less than 50Tb free'
        mocked_check.assert_not_called()
        mocked_disk_usage.return_value.free = 50000000000000

        mocked_check.return_value = ([], [], ['dirty', 'files'])
        with self.assertRaises(EGCGError) as e:
            recall_sample.restore('a_sample_id')

        assert str(e.exception) == "Found 2 dirty files: ['dirty', 'files']"

        mocked_check.return_value = ([], ['unarchived', 'files'], [])
        recall_sample.restore('a_sample_id')
        mocked_log.assert_any_call(
            logging.WARNING,
            'Found %s files not archived. Have they already been restored? %s',
            (2, ['unarchived', 'files'])
        )

        mocked_check.return_value = (['restorable', 'files'], [], [])
        recall_sample.restore('a_sample_id')

        for f in ['restorable', 'files']:
            mocked_recall.assert_any_call(f)

        mocked_patch.assert_called_with('samples', {'data_deleted': 'none'}, 'sample_id', 'a_sample_id')
