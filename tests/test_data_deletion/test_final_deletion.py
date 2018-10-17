import os
from unittest.mock import patch, Mock

from shutil import rmtree

from data_deletion import FinalSample
from egcg_core.exceptions import ArchivingError

from data_deletion.final_data import FinalDataDeleter
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
    'run_elements': run_elements1,
    'data_deleted': 'all'
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


class TestFinalSample(TestProjectManagement):
    config_file = 'example_data_deletion.yaml'

    def setUp(self):
        self.sample = FinalSample(sample1)

    @patch.object(FinalSample, 'raw_data_files', new=['R1.fastq.gz', 'R2.fastq.gz'])
    @patch.object(FinalSample, 'processed_data_files', new=['sample.vcf.gz', 'sample.bam'])
    @patch(ppath + 'is_released')
    @patch(ppath + 'util.find_files', return_value=['a_deletion_dir/a_file'])
    def test_files_to_purge(self, mocked_find_files, mocked_is_released):
        exp = ['a_deletion_dir/a_file', 'R1.fastq.gz', 'R2.fastq.gz', 'sample.vcf.gz', 'sample.bam']
        mocked_is_released.return_value = False
        with self.assertRaises(ArchivingError) as e:
            _ = self.sample.files_to_purge
        assert str(e.exception) == 'Files not yet remove from lustre cannot be removed from tape: ' + str(exp)

        mocked_is_released.return_value = True
        assert self.sample.files_to_purge == exp

    def test_files_to_remove_from_lustre(self):
        assert self.sample.files_to_remove_from_lustre == []

    @patched_patch_entry
    def test_mark_as_deleted(self, mocked_patch):
        self.sample.mark_as_deleted()
        mocked_patch.assert_called_with('samples', {'data_deleted': 'all'}, 'sample_id', 'a_sample')


class TestFinalDataDeleter(TestDeleter):
    file_exts = (
        'bam', 'bam.bai', 'vcf.gz', 'vcf.gz.tbi', 'g.vcf.gz', 'g.vcf.gz.tbi', 'R1.fastq.gz',
        'R2.fastq.gz', 'R1_fastqc.html', 'R2_fastqc.html'
    )
    samples = (
        Mock(sample_id='this', files_to_purge=['folder_this']),
        Mock(sample_id='that', files_to_purge=['folder_that'])
    )

    def setUp(self):
        self.deleter = FinalDataDeleter(self.cmd_args)
        os.makedirs(os.path.join(self.deleter.fastq_dir, 'a_run'), exist_ok=True)
        os.makedirs(os.path.join(self.deleter.projects_dir, 'a_project'), exist_ok=True)
        os.makedirs(os.path.join(self.deleter.project_archive_dir), exist_ok=True)
        os.makedirs(os.path.join(self.deleter.run_archive_dir), exist_ok=True)

    def tearDown(self):
        to_delete = [
            os.path.join(self.deleter.run_archive_dir),
            os.path.join(self.deleter.project_archive_dir),
            os.path.join(self.deleter.projects_dir, 'a_project'),
            os.path.join(self.deleter.fastq_dir, 'a_run'),
        ]
        for d in to_delete:
            if os.path.exists(d):
                rmtree(d)

    @patch.object(FinalSample, 'release_date', new='now')
    @patch.object(FinalDataDeleter, '_manually_deletable_samples')
    def test_deletable_samples(self, mocked_get):
        mocked_get.return_value = []
        assert self.deleter.deletable_samples() == []
        mocked_get.return_value = [{'sample_id': 'this'}, {'sample_id': 'that'}]
        assert [s.sample_data for s in self.deleter.deletable_samples()] == list(reversed(mocked_get.return_value))

    @patch.object(FinalDataDeleter, 'deletion_dir', new='a_deletion_dir')
    @patch.object(FinalDataDeleter, '_execute')
    @patch.object(FinalDataDeleter, '_move_to_unique_file_name')
    def test_setup_samples_for_deletion(self, mocked_move, mocked_execute):
        self.deleter.setup_samples_for_deletion(self.samples)

        mocked_move.assert_any_call('folder_this', 'a_deletion_dir/this')
        mocked_move.assert_any_call('folder_that', 'a_deletion_dir/that')
        mocked_execute.assert_any_call('mkdir -p a_deletion_dir/this')
        mocked_execute.assert_any_call('mkdir -p a_deletion_dir/that')

    @patch.object(FinalDataDeleter, 'deletion_dir', new='a_deletion_dir')
    @patch.object(FinalDataDeleter, 'info')
    def test_setup_dry_run(self, mocked_log):
        self.deleter.dry_run = True
        self.deleter.setup_samples_for_deletion(self.samples)
        mocked_log.assert_any_call(
            'Sample %s has %s files to delete\n%s',
            self.samples[0], 1, 'folder_this'
        )
        mocked_log.assert_any_call(
            'Sample %s has %s files to delete\n%s',
            self.samples[1], 1, 'folder_that'
        )
        mocked_log.assert_any_call('Will run: mv %s %s', 'folder_this', 'a_deletion_dir/this')
        mocked_log.assert_any_call('Will run: mv %s %s', 'folder_that', 'a_deletion_dir/that')

    @patch.object(FinalDataDeleter, 'setup_samples_for_deletion')
    @patch.object(FinalDataDeleter, 'deletable_samples')
    @patch.object(FinalDataDeleter, '_try_archive_run')
    @patch.object(FinalDataDeleter, '_try_archive_project')
    def test_delete(self, mocked_archive_project, mocked_archive_run, mocked_deletable_samples, mocked_setup):
        mocked_deletable_samples.return_value = [
            Mock(sample_id='this', project_id='project1', released_data_folder=None, release_date='2017-01-12', sample_data={'data_deleted': 'on lustre'}, run_elements=run_elements1),
            Mock(sample_id='that', project_id='project1', released_data_folder=None, release_date='2017-02-24', sample_data={'data_deleted': 'on lustre'}, run_elements=run_elements2)
        ]

        self.deleter.limit_samples = ['this']
        self.deleter.dry_run = True
        assert self.deleter.delete_data() == 0
        mocked_setup.assert_called_with(mocked_deletable_samples.return_value[0:1])
        self.deleter.dry_run = False
        self.deleter.delete_data()
        mocked_deletable_samples.return_value[0].mark_as_deleted.assert_called_with()
        mocked_archive_project.assert_called_once_with('project1')
        mocked_archive_run.assert_any_call('a_run')
        mocked_archive_run.assert_any_call('another_run')

    @patch('egcg_core.rest_communication.get_documents', return_value=run_elements1)
    @patch('egcg_core.rest_communication.get_document', return_value=sample1)
    def test_try_archive_run(self, mocked_get_doc, mocked_get_docs):
        assert os.path.exists(os.path.join(self.deleter.fastq_dir, 'a_run'))
        assert not os.path.exists(os.path.join(self.deleter.run_archive_dir, 'a_run'))
        self.deleter._try_archive_run('a_run')
        assert not os.path.exists(os.path.join(self.deleter.fastq_dir, 'a_run'))
        assert os.path.exists(os.path.join(self.deleter.run_archive_dir, 'a_run'))

    @patch('egcg_core.rest_communication.get_documents', return_value=[sample1])
    def test_try_archive_project(self, mocked_get_docs):
        assert os.path.exists(os.path.join(self.deleter.projects_dir, 'a_project'))
        assert not os.path.exists(os.path.join(self.deleter.project_archive_dir, 'a_project'))
        self.deleter._try_archive_project('a_project')
        assert not os.path.exists(os.path.join(self.deleter.projects_dir, 'a_project'))
        assert os.path.exists(os.path.join(self.deleter.project_archive_dir, 'a_project'))

