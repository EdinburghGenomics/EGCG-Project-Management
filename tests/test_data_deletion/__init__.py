from os.path import join
from unittest.mock import patch
from data_deletion import Deleter
from tests import TestProjectManagement


class TestDeleter(TestProjectManagement):
    config_file = 'example_data_deletion.yaml'

    def setUp(self):
        self.deleter = Deleter(self.assets_deletion)

    def test_deletion_dir(self):
        with patch.object(self.deleter.__class__, '_strnow', return_value='t'):
            assert self.deleter.deletion_dir == join(self.deleter.work_dir, '.data_deletion_t')
