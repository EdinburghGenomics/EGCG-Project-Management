import os
from datetime import datetime
from unittest.mock import patch, Mock, PropertyMock
from data_deletion import ProcessedSample
from data_deletion.delivered_data import DeliveredDataDeleter
from egcg_core.exceptions import ArchivingError
from tests import TestProjectManagement
from tests.test_data_deletion import TestDeleter, patched_patch_entry

run_elements1 = [
    {'run_id': 'a_run', 'project_id': 'a_project', 'sample_id': 'a_sample', 'lane': 2},
    {'run_id': 'another_run', 'project_id': 'a_project', 'sample_id': 'a_sample', 'lane': 3}
]
run_elements2 = [
    {'run_id': 'a_run', 'project_id': 'another_project', 'sample_id': 'yet_another_sample', 'lane': 4},
    {'run_id': 'another_run', 'project_id': 'another_project', 'sample_id': 'yet_another_sample', 'lane': 5}
]

sample1 = {
    'sample_id': 'a_sample',
    'release_dir': 'release_1',
    'project_id': 'a_project',
    'user_sample_id': 'a_user_sample_id',
    'most_recent_proc': {'proc_id': 'a_proc_id'},
    'run_elements': run_elements1
}
sample2 = {
    'sample_id': 'yet_another_sample',
    'release_dir': 'release_1',
    'project_id': 'another_project',
    'user_sample_id': 'another_user_sample_id',
    'run_elements': run_elements2
}


# Fake functions and properties
def fake_find_files(*parts):
    return [os.path.join(*[p.replace('*', 'star') for p in parts])]


def fake_find_file(*parts):
    return fake_find_files(*parts)[0]


ppath = 'data_deletion.'


class TestProcessedSample(TestProjectManagement):
    config_file = 'example_data_deletion.yaml'

    def setUp(self):
        self.sample = ProcessedSample(sample1)

    @patch('data_deletion.util.find_fastqs')
    def test_find_fastqs_for_run_element(self, mocked_find_fastqs):
        run_element = self.sample.sample_data['run_elements'][0]
        self.sample._find_fastqs_for_run_element(run_element)
        mocked_find_fastqs.assert_called_with(
            'tests/assets/data_deletion/fastqs/a_run',
            'a_project',
            'a_sample',
            lane=2
        )

    def test_raw_data_files(self):
        with patch('data_deletion.delivered_data.ProcessedSample.run_elements', new=run_elements1), \
             patch('data_deletion.delivered_data.ProcessedSample._find_fastqs_for_run_element',
                   return_value=['path_2_fastq1', 'path_2_fastq2']):
            assert self.sample.raw_data_files == ['path_2_fastq1', 'path_2_fastq2',
                                                   'path_2_fastq1', 'path_2_fastq2']

    @patch(ppath + 'util.find_file', side_effect=fake_find_file)
    def test_processed_data_files(self, mocked_find_file):
        assert self.sample.processed_data_files == [
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id_R1.fastq.gz',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id_R2.fastq.gz',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.bam',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.bam.bai',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.vcf.gz',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.vcf.gz.tbi',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.g.vcf.gz',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.g.vcf.gz.tbi'
        ]

    def test_released_data_folder(self):
        with patch(ppath + 'util.find_files', side_effect=fake_find_files):
            released_data_folder = self.sample.released_data_folder
        assert released_data_folder == 'tests/assets/data_deletion/delivered_data/a_project/star/a_sample'

    @patch(ppath + 'util.find_files', return_value=['a_deletion_dir/a_file'])
    def test_files_to_purge(self, mocked_find_files):
        with patch.object(ProcessedSample, 'released_data_folder', new=None):
            assert self.sample.files_to_purge == []

        del self.sample.__dict__['files_to_purge']

        with patch.object(ProcessedSample, 'released_data_folder', new='a_deletion_dir'):
            assert self.sample.files_to_purge == ['a_deletion_dir/a_file']
            mocked_find_files.assert_called_with('a_deletion_dir', '*')

    @patch.object(ProcessedSample, 'raw_data_files', new=['R1.fastq.gz', 'R2.fastq.gz'])
    @patch.object(ProcessedSample, 'processed_data_files', new=['sample.vcf.gz', 'sample.bam'])
    @patch(ppath + 'is_archived')
    def test_files_to_remove_from_lustre(self, mocked_is_archived):
        exp = ['R1.fastq.gz', 'R2.fastq.gz', 'sample.vcf.gz', 'sample.bam']
        mocked_is_archived.return_value = False
        with self.assertRaises(ArchivingError) as e:
            _ = self.sample.files_to_remove_from_lustre

        assert str(e.exception) == 'Unarchived files cannot be released from Lustre: ' + str(exp)

        mocked_is_archived.return_value = True
        assert self.sample.files_to_remove_from_lustre == exp

    def test_size_of_files(self):
        patched_stat = patch(ppath + 'stat', return_value=Mock(st_ino='123456', st_size=10000))
        patched_purge = patch(ppath + 'ProcessedSample.files_to_purge',
                              new_callable=PropertyMock(return_value=['file1', 'file2']))
        patched_remove = patch(ppath + 'ProcessedSample.files_to_remove_from_lustre',
                               new_callable=PropertyMock(return_value=[]))

        with patched_stat, patched_purge, patched_remove:
            file_size = self.sample.size_of_files
            assert file_size == 10000

    @patched_patch_entry
    def test_mark_as_deleted(self, mocked_patch):
        self.sample.mark_as_deleted()
        mocked_patch.assert_called_with('samples', {'data_deleted': 'on lustre'}, 'sample_id', 'a_sample')


class TestDeliveredDataDeleter(TestDeleter):
    file_exts = (
        'bam', 'bam.bai', 'vcf.gz', 'vcf.gz.tbi', 'g.vcf.gz', 'g.vcf.gz.tbi', 'R1.fastq.gz',
        'R2.fastq.gz', 'R1_fastqc.html', 'R2_fastqc.html'
    )
    samples = (
        Mock(sample_id='this', files_to_purge=['folder_this'], files_to_remove_from_lustre=['a_file'], size_of_files=2),
        Mock(sample_id='that', files_to_purge=['folder_that'], files_to_remove_from_lustre=['another_file'], size_of_files=4)
    )

    def setUp(self):
        self.deleter = DeliveredDataDeleter(self.cmd_args)

    @patch('egcg_core.rest_communication.get_documents', return_value=[{'some': 'data'}])
    def test_manually_deletable_samples(self, mocked_get):
        self.deleter.manual_delete = list(range(25))  # 2 pages worth
        assert self.deleter._manually_deletable_samples() == [{'some': 'data'}, {'some': 'data'}]
        mocked_get.assert_any_call('samples', quiet=True, where={'$or': [{'sample_id': s} for s in range(20)]}, all_pages=True)
        mocked_get.assert_any_call('samples', quiet=True, where={'$or': [{'sample_id': s} for s in range(20, 25)]}, all_pages=True)

    @patch.object(ProcessedSample, 'release_date', new='now')
    @patch.object(DeliveredDataDeleter, '_manually_deletable_samples')
    def test_deletable_samples(self, mocked_get):
        mocked_get.return_value = []
        assert self.deleter.deletable_samples() == []
        mocked_get.return_value = [{'sample_id': 'this'}, {'sample_id': 'that'}]
        assert [s.sample_data for s in self.deleter.deletable_samples()] == list(reversed(mocked_get.return_value))

    @patch.object(DeliveredDataDeleter, '_execute')
    def test_move_to_unique_file_name(self, mocked_exec):
        with patch('uuid.uuid4', return_value='a_uuid'):
            self.deleter._move_to_unique_file_name('this/that', 'other')
            mocked_exec.assert_called_with('mv this/that other/a_uuid_that')

    @patch.object(DeliveredDataDeleter, 'deletion_dir', new='a_deletion_dir')
    @patch.object(DeliveredDataDeleter, '_execute')
    @patch.object(DeliveredDataDeleter, '_move_to_unique_file_name')
    @patch('data_deletion.delivered_data.release_file_from_lustre')
    def test_setup_samples_for_deletion(self, mocked_release, mocked_move, mocked_execute):
        self.deleter.setup_samples_for_deletion(self.samples)

        mocked_release.assert_any_call('a_file')
        mocked_release.assert_any_call('another_file')
        mocked_move.assert_any_call('folder_this', 'a_deletion_dir/this')
        mocked_move.assert_any_call('folder_that', 'a_deletion_dir/that')
        mocked_execute.assert_any_call('mkdir -p a_deletion_dir/this')
        mocked_execute.assert_any_call('mkdir -p a_deletion_dir/that')

    @patch.object(DeliveredDataDeleter, 'deletion_dir', new='a_deletion_dir')
    @patch.object(DeliveredDataDeleter, 'info')
    def test_setup_dry_run(self, mocked_log):
        self.deleter.dry_run = True
        self.deleter.setup_samples_for_deletion(self.samples)
        mocked_log.assert_any_call(
            'Sample %s has %s files to delete and %s files to remove from Lustre (%.2f G)\n%s\n%s',
            self.samples[0], 1, 1, 2 / 1000000000, 'folder_this', 'a_file'
        )
        mocked_log.assert_any_call(
            'Sample %s has %s files to delete and %s files to remove from Lustre (%.2f G)\n%s\n%s',
            self.samples[1], 1, 1, 4 / 1000000000, 'folder_that', 'another_file'
        )
        mocked_log.assert_any_call('Will run: mv %s %s', 'folder_this', 'a_deletion_dir/this')
        mocked_log.assert_any_call('Will run: mv %s %s', 'folder_that', 'a_deletion_dir/that')
        mocked_log.assert_any_call('Will run: %s', 'lfs hsm_release a_file')
        mocked_log.assert_any_call('Will run: %s', 'lfs hsm_release another_file')
        mocked_log.assert_any_call('Will delete %.2f G of data', 6 / 1000000000)

    @patch.object(DeliveredDataDeleter, 'setup_samples_for_deletion')
    @patch.object(DeliveredDataDeleter, 'deletable_samples')
    def test_delete(self, mocked_deletable_samples, mocked_setup):
        mocked_deletable_samples.return_value = [
            Mock(sample_id='this', released_data_folder=None),
            Mock(sample_id='that', released_data_folder=None)
        ]

        self.deleter.limit_samples = ['this']
        self.deleter.dry_run = True
        assert self.deleter.delete_data() == 0
        mocked_setup.assert_called_with(mocked_deletable_samples.return_value[0:1])
        self.deleter.dry_run = False
        self.deleter.delete_data()
        mocked_deletable_samples.return_value[0].mark_as_deleted.assert_called_with()

    def test_auto_deletable_samples(self):
        # FIXME: The test is commented out because the function is disabled
        pass

    def test_old_enough_for_deletion(self):
        with patch.object(DeliveredDataDeleter, '_now', return_value=datetime(2000, 12, 31)):
            o = self.deleter._old_enough_for_deletion
            assert o('2000-10-01')
            assert not o('2000-10-01', 120)
            assert not o('2000-12-01')
