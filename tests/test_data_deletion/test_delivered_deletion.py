import os
from shutil import rmtree
from os.path import join
from unittest.mock import patch

from tests.test_data_deletion import TestDeleter, patches
from data_deletion.delivered_data import DeliveredDataDeleter


patched_now = patch('data_deletion.Deleter._strnow', return_value='t')


class TestDeliveredDataDeleter(TestDeleter):
    samples = [
        {
            'sample_id': 'a_sample',
            'release_dir': 'release_1',
            'project_id': 'a_project',
            'most_recent_proc': {'proc_id': 'a_proc_id'}
        },
        {
            'sample_id': 'yet_another_sample',
            'release_dir': 'release_1',
            'project_id': 'another_project',
            'most_recent_proc': {'proc_id': 'yet_another_proc_id'}
        },
        {
            'sample_id': 'another_sample',
            'release_dir': 'release_2',
            'project_id': 'a_project',
            'most_recent_proc': {'proc_id': 'another_proc_id'}
        }
    ]
    file_exts = (
        'bam', 'bam.bai', 'vcf.gz', 'vcf.gz.tbi', 'g.vcf.gz', 'g.vcf.gz.tbi', 'R1.fastq.gz', 'R2.fastq.gz',
        'R1_fastqc.html', 'R2_fastqc.html'
    )

    def setUp(self):
        os.chdir(os.path.dirname(self.root_path))
        for s in self.samples:
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
        self.deleter = DeliveredDataDeleter(['a_sample', 'yet_another_sample'], self.assets_deletion)
        self.deleter.local_execute_only = True

    def tearDown(self):
        super().tearDown()
        for p in ('a_project', 'another_project'):
            rmtree(join(self.assets_deletion, 'delivered_data', p))

        deletion_script = join(self.assets_deletion, 'data_deletion.pbs')
        if os.path.isfile(deletion_script):
            os.remove(deletion_script)

    @patch('data_deletion.delivered_data.DeliveredDataDeleter.warning')
    @patches.patched_patch_entry
    def test_mark_sample_as_deleted(self, mocked_patch, mocked_warning):
        self.deleter.mark_sample_as_deleted('a_sample_id', None)
        assert len(mocked_patch.call_args_list) == 0
        mocked_warning.assert_called_with('No pipeline process found for ' + 'a_sample_id')
        self.deleter.mark_sample_as_deleted('a_sample_id', 'a_proc_id')
        mocked_patch.assert_called_with('analysis_driver_procs', {'status': 'deleted'}, 'proc_id', 'a_proc_id')

    def test_setup_sample_for_deletion(self):
        deletable_data = join(self.assets_deletion, 'delivered_data', '.data_deletion_t', 'a_project', 'a_sample')
        assert not os.path.isdir(deletable_data)
        with patched_now:
            self.deleter._setup_sample_for_deletion('a_project', 'a_sample')
        assert os.path.isdir(deletable_data)
        assert sorted(os.listdir(deletable_data)) == sorted(['a_sample.' + x for x in self.file_exts])

    @patch('data_deletion.delivered_data.DeliveredDataDeleter.mark_sample_as_deleted')
    def test_delete_data(self, mocked_mark):
        patched_deletables = patch('data_deletion.delivered_data.DeliveredDataDeleter.deletable_samples', return_value=self.samples[0:2])
        with patched_deletables:
            self.deleter.dry_run = True
            assert self.deleter.delete_data() == 0
            self.deleter.dry_run = False

            for s in self.samples:
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
            self.deleter.delete_data()

        mocked_mark.assert_any_call('a_sample', 'a_proc_id')
        mocked_mark.assert_any_call('yet_another_sample', 'yet_another_proc_id')
        assert not os.path.isdir(join(self.assets_deletion, 'delivered_data', 'another_project'))
        assert not os.path.isdir(join(self.assets_deletion, 'delivered_data', 'a_project', 'release_1'))
        assert os.path.isdir(join(self.assets_deletion, 'delivered_data', 'a_project', 'release_2', 'another_sample'))
        os.makedirs(join(self.assets_deletion, 'delivered_data', 'another_project'))

    def test_auto_deletable_samples(self):
        fake_release_map = {
            'this': '2000-12-01',
            'that': '2000-10-01',
            'other': '2000-09-01'
        }
        patched_release_date = patch(
            'data_deletion.delivered_data.clarity.get_sample_release_date',
            new=lambda sample_id: fake_release_map[sample_id]
        )
        test_payload = [
            {
                'sample_id': 'this',
                'proc_status': 'finished'
            },
            {
                'sample_id': 'that',
                'proc_status': 'finished'
            },
            {
                'sample_id': 'other',
                'proc_status': 'aborted'
            }
        ]
        with patched_release_date, patch(patches.patch_get, return_value=test_payload), patches.patched_now:
            assert self.deleter._auto_deletable_samples() == test_payload[1:]

    def test_old_enough_for_deletion(self):
        with patches.patched_now:
            o = self.deleter._old_enough_for_deletion
            assert o('2000-10-01')
            assert not o('2000-10-01', 120)
            assert not o('2000-12-01')
