import os
from shutil import rmtree
from unittest.mock import patch
from contextlib import contextmanager
from integration_tests import IntegrationTest, integration_cfg
from egcg_core import rest_communication
from egcg_core.config import cfg
from data_deletion import client

work_dir = os.path.dirname(__file__)


@contextmanager
def patches():
    _patches = []

    def _patch(ppath, **kwargs):
        _p = patch(ppath, **kwargs)
        _p.start()
        _patches.append(_p)

    _patch('data_deletion.client.load_config')

    yield

    for p in _patches:
        p.stop()


class TestDeleteRawData(IntegrationTest):
    raw_dir = os.path.join(work_dir, 'raw')
    archive_dir = os.path.join(work_dir, 'archives')

    @classmethod
    def setUpClass(cls):
        cfg.content = {
            'executor': integration_cfg['executor'],
            'data_deletion': {
                'raw_data': cls.raw_dir,
                'raw_archives': cls.archive_dir
            }
        }

    def setUp(self):
        super().setUp()

        for d in ('Data', 'Logs', 'Thumbnail_Images', 'some_metadata'):
            subdir = os.path.join(self.raw_dir, 'a_run', d)
            os.makedirs(subdir, exist_ok=True)
            open(os.path.join(subdir, 'some_data.txt'), 'w').close()

        os.makedirs(self.archive_dir, exist_ok=True)
        for x in os.listdir(self.archive_dir):
            rmtree(os.path.join(self.archive_dir, x))

        rest_communication.post_entry(
            'run_elements',
            {
                'run_element_id': 'a_run_element', 'run_id': 'a_run', 'lane': 1, 'project_id': 'a_project',
                'library_id': 'a_library', 'sample_id': 'a_sample', 'reviewed': 'pass'
            }
        )
        rest_communication.post_entry(
            'analysis_driver_procs',
            {'proc_id': 'a_proc', 'dataset_type': 'run', 'dataset_name': 'a_run', 'status': 'finished'}
        )
        rest_communication.post_entry(
            'runs',
            {'run_id': 'a_run', 'analysis_driver_procs': ['a_proc']}
        )

    @staticmethod
    def _run_main(argv):
        with patches():
            client.main(argv + ['--work_dir', work_dir])

    def test_raw_data(self):
        run_dir = os.path.join(self.raw_dir, 'a_run')
        assert os.path.isdir(run_dir)

        with patches():
            self._run_main(['raw'])

        assert not os.path.isdir(run_dir)
        assert os.path.isfile(os.path.join(self.archive_dir, 'a_run', 'some_metadata', 'some_data.txt'))

    def test_unreviewed_raw_data(self):
        rest_communication.patch_entry('run_elements', {'reviewed': 'not reviewed'}, 'run_element_id', 'a_run_element')
        run_dir = os.path.join(self.raw_dir, 'a_run')
        assert os.path.isdir(run_dir)

        with patches():
            self._run_main(['raw'])

        # nothing should have happened
        assert os.path.isdir(run_dir)
        assert not os.path.isdir(os.path.join(self.archive_dir, 'a_run'))
