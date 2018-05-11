from os.path import join
from unittest.mock import Mock, patch
from bin import recall_sample
from tests import TestProjectManagement
from egcg_core.exceptions import EGCGError
import logging

ppath = 'bin.recall_sample.'


def fake_find_file(*parts):
    return join(*parts)


class TestRecall(TestProjectManagement):
    config_file = 'example_data_deletion.yaml'
    fake_file_states = {
        'this.vcf.gz': ['exists', 'archived', 'released'],
        'this.bam': [],
        'this_r1.fastq.gz': ['exists', 'dirty', 'archived'],
        'this_r2.fastq.gz': ['exists', 'archived']
    }

    @patch(ppath + 'rest_communication.get_document')
    @patch(ppath + 'am.archive_states', return_value=['exists', 'archived'])
    @patch(ppath + 'ProcessedSample')
    def test_file_states(self, mocked_sample, mocked_archive_states, mocked_get_doc):
        fastqs = ['sample_1_r1.fastq.gz', 'sample_1_r2.fastq.gz']
        processed_files = ['sample_1.bam', 'sample_1.bam.bai', 'sample_1.vcf.gz', 'sample_1.vcf.gz.tbi']
        mocked_sample.return_value = Mock(raw_data_files=fastqs, processed_data_files=processed_files)

        assert recall_sample.file_states('sample_1') == {f: ['archived', 'exists'] for f in fastqs + processed_files}
        mocked_get_doc.assert_called_with('aggregate/samples', match={'sample_id': 'sample_1'})

    @patch(ppath + 'file_states', return_value=fake_file_states)
    @patch(ppath + 'get_file_list_size', return_value=1000000000)
    @patch('egcg_core.archive_management.archive_states', return_value=[])
    def test_check(self, mocked_archive_states, mocked_file_size, mocked_file_states):
        obs = recall_sample.check('a_sample_id')
        assert obs == (
            ['this.vcf.gz'],
            ['this_r2.fastq.gz'],
            ['this.bam'],
            ['this_r1.fastq.gz']
        )

    @patch(ppath + 'logger._log')
    @patch(ppath + 'rest_communication.patch_entry')
    @patch(ppath + 'am.recall_from_tape')
    @patch(ppath + 'disk_usage')
    @patch(ppath + 'check')
    def test_restore(self, mocked_check, mocked_disk_usage, mocked_recall, mocked_patch, mocked_log):
        mocked_disk_usage.return_value.free = 1
        with self.assertRaises(EGCGError) as e:
            recall_sample.restore('a_sample_id')

        assert str(e.exception) == 'Unsafe to recall: less than 50Tb free'
        mocked_check.assert_not_called()
        mocked_disk_usage.return_value.free = 50000000000000

        mocked_check.return_value = ([], [], [], ['dirty', 'files'])
        with self.assertRaises(EGCGError) as e:
            recall_sample.restore('a_sample_id')

        mocked_log.assert_any_call(logging.ERROR, 'Found %s dirty files: %s', (2, ['dirty', 'files']))
        assert str(e.exception) == 'Found 2 dirty, 0 unarchived files'

        mocked_check.return_value = ([], ['unreleased', 'files'], [], [])
        recall_sample.restore('a_sample_id')
        mocked_log.assert_any_call(logging.WARNING, 'Found %s files not released: %s', (2, ['unreleased', 'files']))

        mocked_check.return_value = ([], [], ['unarchived', 'files'], [])
        with self.assertRaises(EGCGError) as e:
            recall_sample.restore('a_sample_id')

        mocked_log.assert_any_call(logging.ERROR, 'Found %s files not archived: %s', (2, ['unarchived', 'files']))
        assert str(e.exception) == 'Found 0 dirty, 2 unarchived files'

        mocked_check.return_value = (['restorable', 'files'], [], [], [])
        recall_sample.restore('a_sample_id')

        for f in ['restorable', 'files']:
            mocked_recall.assert_any_call(f)

        mocked_patch.assert_called_with('samples', {'data_deleted': 'none'}, 'sample_id', 'a_sample_id')
