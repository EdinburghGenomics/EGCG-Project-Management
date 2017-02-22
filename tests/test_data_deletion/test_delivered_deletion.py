import os
from os.path import join
from shutil import rmtree
from unittest.mock import patch, Mock
from egcg_core.config import cfg
from data_deletion.delivered_data import ProcessedSample, DeliveredDataDeleter
from tests import TestProjectManagement
from tests.test_data_deletion import TestDeleter, patches

run_elements1 = [
    {'run_id': 'a_run', 'project_id': 'a_project', 'sample_id': 'a_sample', 'lane': '2'},
    {'run_id': 'another_run', 'project_id': 'a_project', 'sample_id': 'a_sample', 'lane': '3'}
]
run_elements2 = [
    {'run_id': 'a_run', 'project_id': 'another_project', 'sample_id': 'yet_another_sample', 'lane': '4'},
    {'run_id': 'another_run', 'project_id': 'another_project', 'sample_id': 'yet_another_sample', 'lane': '5'}
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
sample3 = {
    'sample_id': 'another_sample',
    'release_dir': 'release_2',
    'project_id': 'a_project',
    'user_sample_id': 'yet_another_user_sample_id',
    'most_recent_proc': {'proc_id': 'another_proc_id'}
}


# Fake functions and properties
def fake_find_files(*parts):
    return [os.path.join(*[p.replace('*', 'star') for p in parts])]


def fake_find_file(*parts):
    return fake_find_files(*parts)[0]


@property
def fake_run_elements(self):
    return self.sample_data.get('run_elements')


class TestProcessedSample(TestProjectManagement):
    def __init__(self, *args, **kwargs):
        super(TestProcessedSample, self).__init__(*args, **kwargs)
        cfg.load_config_file(os.path.join(self.root_path, 'etc', 'example_data_deletion.yaml'))

    def setUp(self):
        self.sample1 = ProcessedSample(sample_data=sample1)
        self.sample2 = ProcessedSample(sample_data=sample2)

    def test_find_fastqs_for_run_element(self):
        self.sample1._find_fastqs_for_run_element(run_elements1[0])

    def test_raw_data_files(self):
        with patch('data_deletion.delivered_data.ProcessedSample.run_elements', new=run_elements1),\
             patch('data_deletion.delivered_data.ProcessedSample._find_fastqs_for_run_element', return_value=['path_2_fastq1', 'path_2_fastq2']):
            raw_data_files = self.sample1._raw_data_files()
            assert raw_data_files == ['path_2_fastq1', 'path_2_fastq2', 'path_2_fastq1', 'path_2_fastq2']

    @patch('data_deletion.delivered_data.util.find_file', side_effect=fake_find_file)
    def test_processed_data_files(self, mocked_find_file):
        expected_files = [
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id_R1.fastq.gz',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id_R2.fastq.gz',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.bam',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.bam.bai',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.vcf.gz',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.vcf.gz.tbi',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.g.vcf.gz',
            'tests/assets/data_deletion/projects/a_project/a_sample/a_user_sample_id.g.vcf.gz.tbi'
        ]
        processed_files = self.sample1._processed_data_files()
        assert processed_files == expected_files

    @patch('data_deletion.delivered_data.util.find_files', side_effect=fake_find_files)
    def test_released_data_folder(self, mocked_find_files):
        released_data_folder = self.sample1.released_data_folder
        assert released_data_folder == 'tests/assets/data_deletion/delivered_data/a_project/star/a_sample'

    @patch('os.stat', return_value=Mock(st_ino='123456', st_size=10000))
    @patch('os.path.isdir', return_value=False)
    def test_size_of_files(self, mock_is_dir, mock_os_stat):
        with patch.object(ProcessedSample, 'files_to_purge', new=['file1', 'file2']),\
             patch.object(ProcessedSample, 'files_to_remove_from_lustre', new=[]):
            assert self.sample1.size_of_files == 10000

    @patches.patched_patch_entry
    def test_mark_as_deleted(self, mocked_patch):
        self.sample1.mark_as_deleted()
        mocked_patch.assert_called_with('samples', {'data_deleted': 'on lustre'}, 'sample_id', 'a_sample')


class TestDeliveredDataDeleter(TestDeleter):
    def __init__(self, *args, **kwargs):
        super(TestDeliveredDataDeleter, self).__init__(*args, **kwargs)
        self.samples = [ProcessedSample(sample1), ProcessedSample(sample2), ProcessedSample(sample3)]
        self.file_exts = (
            'bam', 'bam.bai', 'vcf.gz', 'vcf.gz.tbi', 'g.vcf.gz', 'g.vcf.gz.tbi', 'R1.fastq.gz', 'R2.fastq.gz',
            'R1_fastqc.html', 'R2_fastqc.html'
        )

    def setUp(self):
        # set up the data delivered directories
        os.chdir(os.path.dirname(self.root_test_path))
        for s in [sample1, sample2, sample3]:
            for x in self.file_exts:
                d = join(
                    self.assets_deletion,
                    'delivered_data',
                    s['project_id'],
                    s['release_dir'],
                    s['sample_id']
                )
                os.makedirs(d, exist_ok=True)
                self.touch(join(d, s['sample_id'] + '.' + x))
        self.deleter = DeliveredDataDeleter(self.assets_deletion)
        self.deleter.local_execute_only = True

        # Set up the raw data
        self.mkdir(join(self.assets_deletion, 'fastqs', 'a_run'))
        self.mkdir(join(self.assets_deletion, 'fastqs', 'another_run'))
        self.touch(join(self.assets_deletion, 'fastqs', 'another_run', 'Undetermined_test1_R1.fastq.gz'))
        self.touch(join(self.assets_deletion, 'fastqs', 'another_run', 'Undetermined_test1_R2.fastq.gz'))

        os.makedirs(join(self.assets_deletion, 'fastqs', 'archive'), exist_ok=True)

    def tearDown(self):
        super().tearDown()
        # Remove the delivered data
        for p in ('a_project', 'another_project'):
            rmtree(join(self.assets_deletion, 'delivered_data', p))

        # Remove the raw data
        for t in (('a_run',), ('another_run',), ('archive', 'a_run'), ('archive', 'another_run')):
            path = join(*((self.assets_deletion, 'fastqs') + t))
            if os.path.exists(path):
                rmtree(path)

    def test_deletable_samples(self):
        pass

    @patch.object(ProcessedSample, 'size_of_files', new=1000000000)
    @patch.object(ProcessedSample, 'files_to_purge', new=['file1', 'file2'])
    @patch.object(ProcessedSample, 'files_to_remove_from_lustre', new=[])
    def test_setup_samples_for_deletion(self):
        self.deleter.setup_samples_for_deletion(self.samples[0:1], dry_run=True)
        with patch('egcg_core.executor.local_execute', return_value=Mock(join=lambda: 0)) as mocked_execute:
            self.deleter.setup_samples_for_deletion(self.samples[0:1], dry_run=False)
            assert mocked_execute.call_count == 3
            expected_deletion_dir = self.deleter.deletion_dir + '/' + self.samples[0].sample_id
            assert mocked_execute.call_args_list[0][0][0] == 'mkdir -p ' + expected_deletion_dir
            # Can't predict exactly the output file name but can test the input file and output dir
            assert mocked_execute.call_args_list[1][0][0].startswith('mv file1 ' + expected_deletion_dir)
            assert mocked_execute.call_args_list[2][0][0].startswith('mv file2 ' + expected_deletion_dir)

    def test_try_archive_run(self):
        assert os.path.exists(join(self.assets_deletion, 'fastqs', 'archive'))
        assert os.path.exists(join(self.assets_deletion, 'fastqs', 'a_run'))
        self.deleter._try_archive_run('a_run')
        assert not os.path.exists(join(self.assets_deletion, 'fastqs', 'a_run'))
        assert os.path.exists(join(self.assets_deletion, 'fastqs', 'archive', 'a_run'))
        assert os.path.exists(join(self.assets_deletion, 'fastqs', 'another_run'))

        self.deleter._try_archive_run('another_run')
        assert not os.path.exists(join(self.assets_deletion, 'fastqs', 'another_run'))
        assert os.path.exists(join(self.assets_deletion, 'fastqs', 'archive', 'another_run'))
        assert not os.path.exists(
            join(self.assets_deletion, 'fastqs', 'archive', 'another_run', 'Undetermined_test1_R1.fastq.gz'))

    @patch.object(ProcessedSample, 'mark_as_deleted')
    @patch('data_deletion.Deleter._strnow', return_value='t')
    def test_delete_data(self, mocked_now, mocked_mark):
        with patch.object(DeliveredDataDeleter, 'deletable_samples', return_value=self.samples[0:2]):
            with patch.object(DeliveredDataDeleter, 'setup_samples_for_deletion'):
                self.deleter.dry_run = True
                assert self.deleter.delete_data() == 0

            self.deleter.dry_run = False
            for s in [sample1, sample2, sample3]:
                assert os.listdir(
                    join(
                        self.assets_deletion,
                        'delivered_data',
                        s['project_id'],
                        s['release_dir'],
                        s['sample_id']
                    )
                )
            assert not os.path.isdir(join(self.assets_deletion, '.data_deletion_t'))
            with patch.object(ProcessedSample, 'run_elements', new=fake_run_elements), \
                 patch('data_deletion.delivered_data.rest_communication.get_documents'):
                self.deleter.delete_data()
                assert mocked_mark.call_count == 2
                assert not os.path.isdir(join(self.assets_deletion, 'delivered_data', 'a_project', 'release_1'))
                assert os.path.isdir(
                    join(self.assets_deletion, 'delivered_data', 'a_project', 'release_2', 'another_sample'))

    def test_auto_deletable_samples(self):
        # FIXME: The test is commented out because the function is disable
        pass

    def test_old_enough_for_deletion(self):
        with patches.patched_now:
            o = self.deleter._old_enough_for_deletion
            assert o('2000-10-01')
            assert not o('2000-10-01', 120)
            assert not o('2000-12-01')
