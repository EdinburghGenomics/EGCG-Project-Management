import os
from shutil import rmtree
from os.path import join
from unittest.mock import patch

from data_deletion.fastq import FastqDeleter, _FastqDeletionRecord
from egcg_core.util import find_files, find_fastqs
from tests.test_data_deletion import TestDeleter, patches as p


class TestFastqDeleter(TestDeleter):
    @property
    def fake_deletion_record(self):
        return _FastqDeletionRecord(
            run_element={
                'run_id': 'a_run',
                'project_id': 'a_project',
                'sample_id': 'deletable_sample',
                'lane': '1'
            },
            fastqs=find_fastqs(
                join(self.assets_deletion, 'fastqs', 'a_run', 'fastq'),
                'a_project',
                'deletable_sample',
                lane=1
            )
        )

    def _setup_fastqs(self, run_id, project_id, sample_id):
        fastq_dir = join(self.assets_deletion, 'fastqs', run_id, 'fastq', project_id, sample_id)
        os.makedirs(fastq_dir, exist_ok=True)
        for lane in range(8):
            for read in ('1', '2'):
                for file_ext in ('fastq.gz', 'fastqc.html'):
                    self.touch(join(fastq_dir, 'fastq_L00%s_R%s.%s' % (str(lane + 1), read, file_ext)))

    def setUp(self):
        os.chdir(os.path.dirname(self.root_test_path))
        self.deleter = FastqDeleter(self.assets_deletion)
        self.deleter.local_execute_only = True
        for run_id in ('a_run', 'another_run'):
            for sample_id in ('deletable_sample', 'non_deletable_sample'):
                self._setup_fastqs(run_id, 'a_project', sample_id)

    def tearDown(self):
        super().tearDown()

        for r in ('a_run', 'another_run'):
            rmtree(join(self.assets_deletion, 'fastqs', r), ignore_errors=True)

    def test_samples_released_in_lims(self):
        with p.patched_clarity_get_samples:
            assert self.deleter.samples_released_in_lims == {'deletable_sample', 'deletable_sample_2'}

    def test_samples_released_in_app(self):
        with p.patched_deletable_samples:
            self.compare_lists(self.deleter.samples_released_in_app, ['deletable_sample'])

    def test_find_fastqs_for_run_element(self):
        run_element = {
            'run_id': 'a_run',
            'project_id': 'a_project',
            'sample_id': 'deletable_sample',
            'lane': '1'
        }
        fqs = self.deleter.find_fastqs_for_run_element(run_element)
        assert len(fqs) == 2

        obs = [os.path.basename(f) for f in fqs]
        expected_fqs = find_fastqs(
            join(self.assets_deletion, 'fastqs', 'a_run', 'fastq'),
            'a_project',
            'deletable_sample',
            lane=1
        )
        exp = [os.path.basename(f) for f in expected_fqs]
        self.compare_lists(obs, exp)

    def test_setup_record_for_deletion(self):
        os.makedirs(self.deleter.deletion_dir, exist_ok=True)
        e = {
            'run_id': 'a_run',
            'project_id': 'a_project',
            'sample_id': 'deletable_sample',
            'lane': '1'
        }
        fqs = self.deleter.find_fastqs_for_run_element(e)
        record = _FastqDeletionRecord(e, fqs)
        self.deleter._setup_record_for_deletion(record)

        self.compare_lists(os.listdir(self.deleter.deletion_dir), ['a_run'])
        self.compare_lists(os.listdir(join(self.deleter.deletion_dir, 'a_run', 'fastq', 'a_project')), ['deletable_sample'])
        self.compare_lists(
            os.listdir(join(self.deleter.deletion_dir, 'a_run', 'fastq', 'a_project', 'deletable_sample')),
            [os.path.basename(fq) for fq in fqs]
        )
        rmtree(self.deleter.deletion_dir)

    def setup_deletion_records(self):  # test set() intersection between lims and app
        with p.patched_clarity_get_samples, p.patched_deletable_samples:
            records = self.deleter.setup_deletion_records()
            assert len(records) == 1 and records[0].sample_id == 'deletable_sample'

    def test_setup_fastqs_for_deletion(self):
        records = [self.fake_deletion_record]
        with p.patched_clarity_get_samples:
            self.deleter.setup_fastqs_for_deletion(records)

    def test_delete_data(self):
        run_elements = []
        for run in ('a_run', 'another_run'):
            for lane in range(8):
                e = {
                    'run_id': run,
                    'project_id': 'a_project',
                    'sample_id': 'deletable_sample',
                    'lane': str(lane + 1)
                }
                run_elements.append(e)

        patched_app = patch(
            'data_deletion.fastq.FastqDeleter.samples_released_in_app',
            new={'deletable_sample'}
        )
        patched_run_elements = patch(
            'data_deletion.fastq.rest_communication.get_documents',
            return_value=run_elements
        )
        patched_mark_sample = patch(
            'data_deletion.fastq.FastqDeleter.mark_sample_as_deleted',
        )
        basenames = []
        for lane in range(8):
            for read in ('1', '2'):
                for file_ext in ('fastq.gz', 'fastqc.html'):
                    basenames.append('fastq_L00%s_R%s.%s' % (lane + 1, read, file_ext))

        with p.patched_clarity_get_samples, patched_app, patched_run_elements, patched_mark_sample:
            self.compare_lists(
                [os.path.basename(f) for f in find_files(self.assets_deletion, 'fastqs', '*run*')],
                ['a_run', 'another_run']
            )
            for run in ('a_run', 'another_run'):
                self.compare_lists(
                    os.listdir(join(self.assets_deletion, 'fastqs', run, 'fastq', 'a_project', 'deletable_sample')),
                    basenames
                )
            self.deleter.delete_data()

            for run in ('a_run', 'another_run'):
                self.compare_lists(
                    os.listdir(join(self.assets_deletion, 'fastqs', run, 'fastq', 'a_project', 'deletable_sample')),
                    [f for f in basenames if f.endswith('fastqc.html')]
                )
