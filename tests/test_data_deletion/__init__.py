from tests import TestProjectManagement
from data_deletion import Deleter


class TestDeleter(TestProjectManagement):
    config_file = 'example_data_deletion.yaml'

    def setUp(self):
        self.deleter = Deleter(self.assets_deletion)
