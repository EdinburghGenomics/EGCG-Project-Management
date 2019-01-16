from unittest.mock import patch
from data_deletion.DMF_data import DMFDataDeleter
from tests.test_data_deletion import TestDeleter

ppath = 'data_deletion.'


class TestDMFDataDeleter(TestDeleter):
    def setUp(self):
        self.deleter = DMFDataDeleter(self.cmd_args)

    def test_get_cmd_output(self):
        exist_status, stdout, stderr = self.deleter._get_cmd_output('ls %s' % self.deleter.dmf_file_system)
        assert exist_status == 0
        assert stdout == b'afid\n'
        assert stderr == b''

        exist_status, stdout, stderr = self.deleter._get_cmd_output('ls non_existing_directory')
        assert exist_status != 0  # exist status seems to be os dependent
        assert stdout == b''
        assert b'No such file or directory' in stderr

    @patch.object(DMFDataDeleter, '_get_cmd_output', return_value=(2, b'', b'No such file or directory'))
    def test_find_files_to_delete(self, mock_cmd_out):
        files_to_delete = self.deleter.find_files_to_delete()
        assert files_to_delete == ['tests/assets/dmf_filesystem/afid']
        mock_cmd_out.assert_called_once_with('lfs fid2path tests/assets/lustre_file_system afid')
