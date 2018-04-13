import hashlib
import os
from os.path import join, abspath, dirname
from unittest import TestCase
from unittest.mock import Mock


class NamedMock(Mock):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

class TestProjectManagement(TestCase):
    root_path = abspath(dirname(dirname(__file__)))
    etc_config = join(root_path, 'etc', 'example_project_management.yaml')
    root_test_path = join(root_path, 'tests')
    assets_path = join(root_test_path, 'assets')
    assets_deletion = join(assets_path, 'data_deletion')

    @staticmethod
    def touch(file_path):
        open(file_path, 'w').close()

    @staticmethod
    def md5(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        with open(fname + '.md5', "w") as f:
            f.write(hash_md5.hexdigest() + "  " + fname)

    @staticmethod
    def mkdir(file_path):
        os.makedirs(file_path, exist_ok=True)

    def compare_lists(self, obs, exp):
        obs = sorted(obs)
        exp = sorted(exp)
        if obs != exp:
            print('observed:')
            print(obs)
            print('expected:')
            print(exp)
            raise AssertionError
