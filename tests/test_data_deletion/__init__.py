import os
from shutil import rmtree
from os.path import join
from egcg_core.config import cfg
from tests import TestProjectManagement
from unittest.mock import patch, Mock
from egcg_core.util import find_files
from data_deletion import Deleter


class FakeExecutor(Mock):
    @staticmethod
    def join():
        pass


class TestDeleter(TestProjectManagement):
    def __init__(self, *args, **kwargs):
        super(TestDeleter, self).__init__(*args, **kwargs)
        cfg.load_config_file(os.path.join(self.root_path, 'etc', 'example_data_deletion.yaml'))

    def setUp(self):
        self.deleter = Deleter(self.assets_deletion)

    @patch('data_deletion.executor.local_execute', return_value=FakeExecutor())
    def test_execute(self, mocked_execute):
        self.deleter._execute('a test command')
        mocked_execute.assert_called_with('a test command')

    def tearDown(self):
        deletion_script = join(self.assets_deletion, 'data_deletion.pbs')
        if os.path.isfile(deletion_script):
            os.remove(deletion_script)
        for tmpdir in find_files(self.assets_deletion, '*', '.data_deletion_*'):
            rmtree(tmpdir)

    @patch('egcg_core.notifications.log.LogNotification.notify')
    def test_crash_report(self, mocked_notify):
        patched_delete = patch.object(self.deleter.__class__, 'delete_data', side_effect=ValueError('Something broke'))
        with patch('sys.exit'), patched_delete:
            self.deleter.run()

        assert 'ValueError: Something broke' in mocked_notify.call_args[0][0]
