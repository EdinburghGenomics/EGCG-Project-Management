import os
from shutil import rmtree
from os.path import join, abspath, dirname
from unittest import TestCase
from unittest.mock import patch, Mock
from egcg_core.util import find_files
print('wd: ' + os.getcwd())
from data_deletion import Deleter


class FakeExecutor(Mock):
    @staticmethod
    def join():
        pass


class TestDeleter(TestCase):
    assets_path = join(abspath(dirname(__file__)), 'assets')
    assets_deletion = join(assets_path, 'data_deletion')

    @staticmethod
    def touch(file_path):
        open(file_path, 'w').close()

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
    
    def compare_lists(self, obs, exp):
        obs = sorted(obs)
        exp = sorted(exp)
        if obs != exp:
            print('observed:')
            print(obs)
            print('expected:')
            print(exp)
            raise AssertionError
