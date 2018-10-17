import os
from unittest.mock import patch
from datetime import datetime
from egcg_core.config import cfg
from bin.detect_sample_to_delete import SampleToDeleteDetector
from tests import TestProjectManagement


def _build_sample_data(list_delivered, list_downloaded):
    return {
        'files_downloaded': [{'file_path': f} for f in list_downloaded],
        'files_delivered': [{'file_path': f} for f in list_delivered]
    }


sample1 = {'sample_id': 'sample1', 'project_id': 'project1'}
sample2 = {'sample_id': 'sample2', 'project_id': 'project1'}
sample1.update(_build_sample_data(['path/to/file1'], ['path/to/file1', 'path/to/file2']))
sample2.update(_build_sample_data(['path/to/file1', 'path/to/file2'], ['path/to/file1']))

statuses = [
    {'processes': [{'name': 'Data Release EG 2.0 ST', 'date': 'Sep 15 2017'}]},
    {'processes': [{'name': 'a process', 'date': 'Oct 02 2017'}]}
]


class TestSampleToDeleteDetector(TestProjectManagement):
    config_file = 'example_data_deletion.yaml'

    def setUp(self):
        self.detector = SampleToDeleteDetector()

    def test_get_status_from_sample(self):
        with patch('bin.detect_sample_to_delete.rest_communication.get_documents') as patched_get_docs:
            patched_get_docs.return_value = [
                {'sample_id': 'a_sample1', 'statuses': ['status1', 'status2']},
                {'sample_id': 'a_sample2', 'statuses': ['status3', 'status4']}
            ]
            assert self.detector._get_status_from_sample('a_project', 'a_sample1') == ['status1', 'status2']

            patched_get_docs.assert_called_once_with(
                'lims/sample_status', match={'project_id': 'a_project', 'project_status': 'all'}, quiet=True
            )
            assert 'a_sample1' in self.detector._cache_sample_to_lims_statuses
            assert 'a_sample2' in self.detector._cache_sample_to_lims_statuses
            assert ['status1', 'status2'] == self.detector._cache_sample_to_lims_statuses['a_sample1']
            assert ['status3', 'status4'] == self.detector._cache_sample_to_lims_statuses['a_sample2']

        # This get info from the cache so no need for the patch
        assert self.detector._get_status_from_sample('a_project', 'a_sample2') == ['status3', 'status4']

    def test_get_release_date_from_sample_statuses(self):
        assert self.detector._get_release_date_from_sample_statuses(statuses) == datetime(year=2017, month=9, day=15)

    def test_get_release_date(self):
        with patch.object(SampleToDeleteDetector, '_get_status_from_sample', return_value=statuses):
            assert self.detector._get_release_date('a_project', 'a_sample1') == datetime(year=2017, month=9, day=15)
        with patch.object(SampleToDeleteDetector, '_get_status_from_sample'), \
                patch('bin.detect_sample_to_delete.clarity.get_sample_release_date') as patch_release_date:
            patch_release_date.return_value = '2017-09-12'
            assert self.detector._get_release_date('a_project', 'a_sample2') == datetime(year=2017, month=9, day=12)

    def test_download_confirmation(self):
        assert self.detector._download_confirmation(_build_sample_data(['path/to/file1'], ['path/to/file1']))
        assert not self.detector._download_confirmation(_build_sample_data(['path/to/file1', 'path/to/file2'], ['path/to/file1']))
        assert self.detector._download_confirmation(_build_sample_data(['path/to/file1'], ['path/to/file1', 'path/to/file2']))

    def test_check_samples_final_deletion(self):
        with patch('bin.detect_sample_to_delete.rest_communication.get_documents') as patched_get_docs, \
                patch('bin.detect_sample_to_delete._utcnow', return_value=datetime(2018, 1, 1)), \
                patch.object(SampleToDeleteDetector, 'info') as mock_info:
            patched_get_docs.return_value = [sample1, sample2]
            self.detector._cache_sample_to_release_date = {
                'sample1': datetime(year=2016, month=2, day=27),
                'sample2': datetime(year=2017, month=10, day=12)
            }
            self.detector.check_samples_final_deletion()
            mock_info.assert_called_with('%s\t%s\t%s', 'project1', '2016-02-27', 'sample1')

    def test_check_deletable_samples(self):
        with patch('bin.detect_sample_to_delete.rest_communication.get_documents') as patched_get_doc, \
             patch('bin.detect_sample_to_delete.send_plain_text_email') as patch_send_email, \
             patch('bin.detect_sample_to_delete._utcnow', return_value=datetime(2017, 1, 1)):
            patched_get_doc.return_value = [sample1, sample2]
            self.detector._cache_sample_to_release_date = {
                'sample1': datetime(year=2016, month=2, day=27),
                'sample2': datetime(year=2016, month=2, day=27)
            }
            os.makedirs(cfg['data_deletion']['log_dir'], exist_ok=True)
            self.detector.check_deletable_samples(90)
            candidate_file = os.path.join(
                cfg['data_deletion']['log_dir'],
                'Candidate_samples_for_deletion_gt_90_days_old_2017-01-01.csv'
            )
            msg = '''Hi,
The attached csv file contains all samples ready for deletion on the 2017-01-01.
Please review them and get back to the bioinformatics team with samples that can be deleted.
'''
            patch_send_email.assert_called_once_with(
                attachments=[candidate_file], mailhost='smtp.mail.com', msg=msg, port=25,
                recipients=['recipient@email.com'], sender='sender@email.com', subject='Samples ready for deletion'
            )

            assert os.path.exists(candidate_file)
            content = """Project id,Release date,Nb sample confirmed,Nb sample not confirmed,Download not confirmed,Download confirmed
project1,2016-02-27,1,1,sample2,sample1
"""
            with open(candidate_file) as open_file:
                assert open_file.read() == content

            os.unlink(candidate_file)
