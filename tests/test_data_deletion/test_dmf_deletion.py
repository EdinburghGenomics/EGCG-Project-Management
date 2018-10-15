import os
from datetime import datetime
from unittest.mock import patch, Mock, PropertyMock
from data_deletion import ProcessedSample
from data_deletion.DMF_data import DMFDtatDeleter
from data_deletion.delivered_data import DeliveredDataDeleter
from egcg_core.exceptions import ArchivingError
from tests import TestProjectManagement
from tests.test_data_deletion import TestDeleter, patched_patch_entry

ppath = 'data_deletion.'


class TestDMFDtatDeleter(TestDeleter):
    file_exts = (
        'bam', 'bam.bai', 'vcf.gz', 'vcf.gz.tbi', 'g.vcf.gz', 'g.vcf.gz.tbi', 'R1.fastq.gz',
        'R2.fastq.gz', 'R1_fastqc.html', 'R2_fastqc.html'
    )
    samples = (
        Mock(sample_id='this', files_to_purge=['folder_this'], files_to_remove_from_lustre=['a_file'], size_of_files=2),
        Mock(sample_id='that', files_to_purge=['folder_that'], files_to_remove_from_lustre=['another_file'], size_of_files=4)
    )

    def setUp(self):
        self.deleter = DMFDtatDeleter(self.cmd_args)

    def test_get_cmd_output(self):
        exist_status, stdout, stderr = self.deleter._get_cmd_output('ls %s' % self.deleter.dmf_file_system)
        assert exist_status == 0
        assert stdout == b'afid\n'
        assert stderr == b''

        exist_status, stdout, stderr = self.deleter._get_cmd_output('ls non_existing_directory')
        assert exist_status == 1
        assert stdout == b''
        assert stderr == b'ls: non_existing_directory: No such file or directory\n'


    @patch.object(DMFDtatDeleter, '_get_cmd_output', return_value=(2, b'', b'No such file or directory'))
    def test_find_files_to_delete(self, mock_cmd_out):
        files_to_delete = self.deleter.find_files_to_delete()
        assert files_to_delete == ['tests/assets/dmf_filesystem/afid']
        mock_cmd_out.assert_called_once_with('lfs fid2path tests/assets/lustre_file_system afid')



