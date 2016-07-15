from os.path import join, abspath, dirname
from unittest import TestCase


class TestProjectManagement(TestCase):
    root_path = abspath(dirname(__file__))
    assets_path = join(root_path, 'assets')
    assets_deletion = join(assets_path, 'data_deletion')

    @staticmethod
    def touch(file_path):
        open(file_path, 'w').close()

    def compare_lists(self, obs, exp):
        obs = sorted(obs)
        exp = sorted(exp)
        if obs != exp:
            print('observed:')
            print(obs)
            print('expected:')
            print(exp)
            raise AssertionError
