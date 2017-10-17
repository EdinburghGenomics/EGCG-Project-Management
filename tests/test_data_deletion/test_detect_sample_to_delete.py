import os
from os.path import join
from shutil import rmtree
from unittest.mock import patch, Mock, PropertyMock

from datetime import datetime
from egcg_core.config import cfg

from bin.detect_sample_to_delete import old_enough_for_deletion, download_confirmation, check_deletable_samples
from data_deletion import ProcessedSample
from data_deletion.delivered_data import DeliveredDataDeleter
from tests import TestProjectManagement
from tests.test_data_deletion import TestDeleter, patches


def _build_sample_data(list_delivered, list_downloaded):
    return {
        'files_downloaded': [{'file_path': f} for f in list_downloaded],
        'files_delivered': [{'file_path': f} for f in list_delivered]
    }

sample1 = {
    'sample_id': 'sample1',
    'project_id': 'project1'
}
sample1.update(_build_sample_data(['path/to/file1'], ['path/to/file1', 'path/to/file2']))

sample2 = {
    'sample_id': 'sample2',
    'project_id': 'project1'
}
sample2.update(_build_sample_data(['path/to/file1', 'path/to/file2'], ['path/to/file1']))

class TestDetectSample(TestProjectManagement):

    def setUp(self):
        cfg.load_config_file(os.path.join(self.root_path, 'etc', 'example_data_deletion.yaml'))
        # Make sure we are in the root dir so the test assets are available
        os.chdir(self.root_path)

    def test_old_enough_for_deletion(self):
        with patch('bin.detect_sample_to_delete._utcnow', return_value=datetime(2017, 1, 1)):
            assert old_enough_for_deletion(date_run='2016-10-01', age_threshold=90)
            assert not old_enough_for_deletion(date_run='2016-10-05', age_threshold=90)
            assert old_enough_for_deletion(date_run='2016-10-05', age_threshold=60)

    def test_download_confirmation(self):
        assert download_confirmation(_build_sample_data(['path/to/file1'], ['path/to/file1']))
        assert not download_confirmation(_build_sample_data(['path/to/file1', 'path/to/file2'], ['path/to/file1']))
        assert download_confirmation(_build_sample_data(['path/to/file1'], ['path/to/file1', 'path/to/file2']))

    def test_check_deletable_samples(self):
        with patch('bin.detect_sample_to_delete.rest_communication.get_documents') as patched_get_doc, \
             patch('bin.detect_sample_to_delete.send_email') as patch_send_email, \
             patch('bin.detect_sample_to_delete.clarity.get_sample_release_date') as patch_release_date, \
             patch('bin.detect_sample_to_delete._utcnow', return_value=datetime(2017, 1, 1)):
            patched_get_doc.return_value = [sample1, sample2]
            patch_release_date.return_value = '2016-09-12'
            os.makedirs(cfg.query('data_deletion', 'log_dir'), exist_ok=True)
            check_deletable_samples(90)
            candidate_file = os.path.join(
                cfg.query('data_deletion', 'log_dir'),
                'Candidate_samples_for_deletion_gt_90_days_old_2017-01-01.csv'
            )
            msg = '''Hi,
The attached csv file contains all samples ready for deletion on the 2017-01-01.
Please review them and get back to the bioinformatics team with samples that can be deleted.
'''
            patch_send_email.assert_called_once_with(
                attachments=[candidate_file], mailhost='smtp.mail.com', msg=msg, port=25,
                recipients=['recipient@email.com'], sender='sender@email.com', strict=True,
                subject='Samples ready for deletion'
            )

            assert os.path.exists(candidate_file)
            content = """Project id,Release date,Nb sample confirmed,Nb sample not confirmed,Download not confirmed,Download confirmed
project1,2016-09-12,1,1,sample2,sample1
"""
            with open(candidate_file) as open_file:
                assert open_file.read() == content

            os.unlink(candidate_file)



