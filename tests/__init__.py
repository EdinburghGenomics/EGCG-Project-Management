import os
from os.path import join, abspath, dirname
from unittest import TestCase


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
