import os
from shutil import rmtree
from os.path import join
from tests import TestProjectManagement
from unittest.mock import patch, Mock
from egcg_core.util import find_files
from data_deletion import Deleter


class FakeExecutor(Mock):
    @staticmethod
    def join():
        pass


class TestDeleter(TestProjectManagement):
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
