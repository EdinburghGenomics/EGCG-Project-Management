import hashlib
from os.path import join, dirname
from egcg_core.config import cfg
from unittest import TestCase
from unittest.mock import Mock


class NamedMock(Mock):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

        
class TestProjectManagement(TestCase):
    root_path = dirname(dirname(__file__))
    etc_config = join(root_path, 'etc', 'example_project_management.yaml')
    root_test_path = join(root_path, 'tests')
    assets_path = join(root_test_path, 'assets')
    assets_deletion = join(assets_path, 'data_deletion')
    config_file = 'example_project_management.yaml'

    @classmethod
    def setUpClass(cls):
        cfg.load_config_file(join(cls.root_path, 'etc', cls.config_file))

    @staticmethod
    def md5(fname):
        hash_md5 = hashlib.md5()
        with open(fname, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        with open(fname + '.md5', 'w') as f:
            f.write(hash_md5.hexdigest() + '  ' + fname)

    @staticmethod
    def compare_lists(obs, exp):
        obs = sorted(obs)
        exp = sorted(exp)
        if obs != exp:
            print('observed:')
            print(obs)
            print('expected:')
            print(exp)
            raise AssertionError
