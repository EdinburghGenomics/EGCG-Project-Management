from unittest.mock import patch

from egcg_core.archive_management import archive_states, release_file_from_lustre, ArchivingError
from tests import TestProjectManagement


class TestArchiveManagement(TestProjectManagement):
    def test_archive_states(self):

        with patch('data_deletion.archive_management._get_stdout',
                   return_value='testfile: (0x0000000d) released exists archived, archive_id:1'):
            assert archive_states('testfile') == ['released', 'exists', 'archived']

        with patch('data_deletion.archive_management._get_stdout',
                   return_value='testfile: (0x00000009) exists archived, archive_id:1'):
            assert archive_states('testfile') == ['exists', 'archived']

        with patch('data_deletion.archive_management._get_stdout',
                   return_value='testfile: (0x00000001) exists, archive_id:1'):
            assert archive_states('testfile') == ['exists']

        with patch('data_deletion.archive_management._get_stdout',
                   return_value='testfile: (0x00000000)'):
            assert archive_states('testfile') == []

    def test_release_file_from_lustre(self):
        with patch('data_deletion.archive_management._get_stdout',
                   side_effect=[
                       'testfile: (0x00000009) exists archived, archive_id:1',
                       'testfile: (0x00000009) exists archived, archive_id:1',
                       ''
                   ]):
            assert release_file_from_lustre('testfile')

        with patch('data_deletion.archive_management._get_stdout',
                   side_effect=[
                       'testfile: (0x0000000d) released exists archived, archive_id:1',
                       'testfile: (0x0000000d) released exists archived, archive_id:1',
                       ''
                   ]):
            assert release_file_from_lustre('testfile') is None

        with patch('data_deletion.archive_management._get_stdout',
                   side_effect=['testfile: (0x00000009) exists, archive_id:1']):
            self.assertRaises(ArchivingError, release_file_from_lustre, 'testfile')





