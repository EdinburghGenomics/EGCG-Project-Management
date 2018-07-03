import os
from shutil import rmtree
from os.path import join
from datetime import datetime
from unittest.mock import patch
from egcg_core.executor import local_execute
from data_deletion.raw_data import RawDataDeleter
from tests.test_data_deletion import TestDeleter, patched_patch_entry


def fake_execute(cmd, cluster_execution=False):
    local_execute(cmd).join()


patched_deletable_runs = patch(
    'data_deletion.raw_data.RawDataDeleter.deletable_runs',
    return_value=[{'run_id': 'deletable_run', 'aggregated': {'most_recent_proc': {'proc_id': 'most_recent_proc'}}}]
)


class TestRawDataDeleter(TestDeleter):
    def _setup_run(self, run_id, deletable_sub_dirs):
        for d in deletable_sub_dirs + ('Stats', 'InterOp', 'RTAComplete.txt'):
            os.makedirs(join(self.assets_deletion, 'raw', run_id, d), exist_ok=True)

    def setUp(self):
        with patch.object(RawDataDeleter, '_now', return_value=datetime(2018, 6, 14, 12)):
            self.deleter = RawDataDeleter(self.cmd_args)

        self.deleter._execute = fake_execute
        self._setup_run('deletable_run', self.deleter.deletable_sub_dirs)
        os.makedirs(join(self.assets_deletion, 'archive'), exist_ok=True)

    def tearDown(self):
        super().tearDown()
        rmtree(join(self.assets_deletion, 'raw', 'deletable_run'), ignore_errors=True)
        for d in os.listdir(join(self.assets_deletion, 'archive')):
            rmtree(join(self.assets_deletion, 'archive', d))

    @patch('egcg_core.rest_communication.get_documents')
    @patch('egcg_core.rest_communication.get_document')
    def test_deletable_runs(self, mocked_get_doc, mocked_get_docs):
        mocked_get_doc.return_value = {
            'run_id': 'a_manually_deletable_recent_run',
            # 'aggregated': {'most_recent_proc': {'status': 'stuck in processing because something broke'}}
        }
        mocked_get_docs.side_effect = [
            [
                {'run_id': 'a_finished_reviewed_recent_run'},
                {'run_id': 'a_finished_reviewed_old_run'},
                {'run_id': 'a_finished_unreviewed_old_run'},
                {'run_id': 'an_extra_run'}
            ],
            [{'useable_date': '12_06_2018_12:00:00'}],
            [{'useable_date': '31_05_2018_12:00:00'}],
            [{'useable_date': '31_05_2018_12:00:00'}, {'useable_date': None}],
            [{'useable_date': '31_05_2018_12:00:00'}]
        ]

        self.deleter.manual_delete = ['a_manually_deletable_recent_run']
        self.deleter.deletion_limit = 2
        runs = self.deleter.deletable_runs()
        mocked_get_doc.assert_called_with('runs', where={'run_id': 'a_manually_deletable_recent_run'})

        obs = [r['run_id'] for r in runs]
        assert obs == ['a_manually_deletable_recent_run', 'a_finished_reviewed_old_run']

    def test_setup_run_for_deletion(self):
        subdirs_to_delete = self.deleter._setup_run_for_deletion('deletable_run')
        self.compare_lists(subdirs_to_delete, self.deleter.deletable_sub_dirs)
        rmtree(self.deleter.deletion_dir)

    def test_setup_runs_for_deletion(self):
        with patched_deletable_runs:
            self.deleter.setup_runs_for_deletion(self.deleter.deletable_runs())
            assert os.listdir(self.deleter.deletion_dir) == ['deletable_run']
        rmtree(self.deleter.deletion_dir)

    def test_delete_runs(self):
        with patched_deletable_runs:
            self.deleter.setup_runs_for_deletion(self.deleter.deletable_runs())
            self.deleter.delete_dir(self.deleter.deletion_dir)
            assert not os.path.isdir(self.deleter.deletion_dir)

    @patched_patch_entry
    def test_mark_run_as_deleted(self, mocked_patch):
        run_object = {
            'run_id': 'a_run',
            'aggregated': {'most_recent_proc': {'proc_id': 'a_most_recent_proc'}}
        }
        self.deleter.mark_run_as_deleted(run_object)
        mocked_patch.assert_called_with(
            'analysis_driver_procs',
            {'status': 'deleted'},
            'proc_id',
            'a_most_recent_proc'
        )

    def test_archive_run(self):
        run_id = 'run_to_archive'
        raw_dir = join(self.assets_deletion, 'raw', run_id)
        self._setup_run(run_id, deletable_sub_dirs=())

        self.deleter.archive_run(run_id)
        assert not os.path.isdir(raw_dir)
        archived_run = join(self.assets_deletion, 'archive', run_id)
        assert os.path.isdir(archived_run)
        self.compare_lists(
            os.listdir(archived_run),
            ['InterOp', 'RTAComplete.txt', 'Stats']
        )
        rmtree(archived_run)

    @patched_patch_entry
    @patched_deletable_runs
    def test_run_deletion(self, mocked_deletable_runs, mocked_patch):
        self._setup_run('non_deletable_run', self.deleter.deletable_sub_dirs)
        del_dir = join(self.assets_deletion, 'raw', 'deletable_run')
        non_del_dir = join(self.assets_deletion, 'raw', 'non_deletable_run')
        self.compare_lists(
            os.listdir(join(self.assets_deletion, 'raw')),
            ['deletable_run', 'non_deletable_run']
        )
        for d in (del_dir, non_del_dir):
            self.compare_lists(
                os.listdir(d),
                list(self.deleter.deletable_sub_dirs) + ['Stats', 'InterOp', 'RTAComplete.txt']
            )
        self.deleter.delete_data()
        assert os.path.isdir(non_del_dir)
        self.compare_lists(
            os.listdir(non_del_dir),
            list(self.deleter.deletable_sub_dirs) + ['Stats', 'InterOp', 'RTAComplete.txt']
        )
        self.compare_lists(
            os.listdir(join(self.assets_deletion, 'archive', 'deletable_run')),
            ['Stats', 'InterOp', 'RTAComplete.txt']
        )
        mocked_patch.assert_called_with(
            'analysis_driver_procs',
            {'status': 'deleted'},
            'proc_id',
            'most_recent_proc'
        )
        assert mocked_deletable_runs.call_count == 1
