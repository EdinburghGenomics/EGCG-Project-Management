import os

from bin.confirm_delivery import parse_aspera_reports, list_file_delivered
from tests import TestProjectManagement


class TestDataDelivery(TestProjectManagement):


    def test_parse_aspera_reports(self):
        aspera_report = os.path.join(self.assets_path, 'confirm_delivery', 'filesreport_test.csv')
        file_list = parse_aspera_reports(aspera_report)
        assert len(file_list) == 422

    def test_list_file_delivered(self):
        list_file_delivered(self.assets_path)
