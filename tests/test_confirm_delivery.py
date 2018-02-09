import datetime
import os
import shutil
from os.path import join
from unittest.mock import patch, Mock

from egcg_core.config import cfg
from pyclarity_lims.entities import ProtocolStep, Artifact

from bin.confirm_delivery import parse_aspera_reports, DeliveredSample, ConfirmDelivery
from tests import TestProjectManagement

sample1 = {
    'sample_id': 'sample1',
    'files_delivered': [
        {'file_path': 'path/to/file.bam', 'md5': 'md5stringforbam', 'size': 10000},
        {'file_path': 'path/to/file.g.vcf.gz', 'md5': 'md5stringforvcf', 'size': 1024}
    ],
    'project_id': 'project1'
}

sample2 = {
    'sample_id': 'sample1',
    'project_id': 'project1',
    'files_delivered': []
}

sample3 = {
    'sample_id': 'sample3',
    'files_delivered': [
        {'file_path': 'path/to/file.bam', 'md5': 'md5stringforbam'},
        {'file_path': 'path/to/file.g.vcf.gz', 'md5': 'md5stringforvcf'}
    ],
    'files_downloaded': [
        {'date': '', 'user': 'testuser', 'file_path': 'path/to/file.bam'},
        {'date': '', 'user': 'testuser', 'file_path': 'path/to/file.g.vcf.gz'}
    ],
    'project_id': 'project1'
}


class TestDeliveredSample(TestProjectManagement):
    def setUp(self):
        cfg.load_config_file(os.path.join(self.root_path, 'etc', 'example_data_delivery.yaml'))
        self.sample = DeliveredSample('sample1')
        # create delivered data in delivery destination
        delivery_dir = os.path.abspath(cfg.query('delivery', 'dest'))
        self.dir_to_delete = [join(delivery_dir, 'project1')]
        self.dir_to_create = [
            join(delivery_dir, 'project1', 'date_delivery', 'sample1')
        ]
        for d in self.dir_to_create:
            self.mkdir(d)

        self.file_to_create = [
            join(delivery_dir, 'project1', 'date_delivery', 'sample1', 'sample1.bam')
        ]
        for f in self.file_to_create:
            self.touch(f)
            self.md5(f)

    def tearDown(self):
        for d in self.dir_to_delete:
            shutil.rmtree(d)

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    def test_data(self, patched_get_doc):
        data = self.sample.data
        assert data['sample_id'] == 'sample1'
        patched_get_doc.assert_called_with('samples', where={'sample_id': 'sample1'})

    @patch('bin.confirm_delivery.patch_entry')
    def test_upload_list_file_delivered(self, patched_get_patch_entry):
        self.sample.delivery_dir = join(self.assets_path, 'data_delivery', 'source')
        list_files = [
            join(self.assets_path, 'data_delivery', 'source', 'test_project', 'deliverable_sample', 'user_s_id.bam'),
            join(self.assets_path, 'data_delivery', 'source', 'test_project', 'deliverable_sample',
                 'user_s_id.g.vcf.gz'),
        ]

        self.sample.upload_list_file_delivered(list_files)
        patched_get_patch_entry.assert_called_with(
            'samples',
            element_id='sample1',
            id_field='sample_id',
            update_lists=['files_delivered'],
            payload={'files_delivered': [
                {'file_path': 'test_project/deliverable_sample/user_s_id.bam',
                 'md5': 'd41d8cd98f00b204e9800998ecf8427e', 'size': 0},
                {'file_path': 'test_project/deliverable_sample/user_s_id.g.vcf.gz',
                 'md5': 'd41d8cd98f00b204e9800998ecf8427e', 'size': 0}
            ]}
        )

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    def test_list_file_delivered_from_data(self, patched_get_doc):
        files_delivered = self.sample.list_file_delivered
        assert files_delivered == sample1.get('files_delivered')
        patched_get_doc.assert_called_with('samples', where={'sample_id': 'sample1'})

    @patch('bin.confirm_delivery.get_document', return_value=sample2)
    @patch('bin.confirm_delivery.patch_entry')
    def test_list_file_delivered_no_data(self, patched_patch_entry, patched_get_doc):
        files_delivered = self.sample.list_file_delivered
        assert files_delivered == [
            {'file_path': 'project1/date_delivery/sample1/sample1.bam', 'md5': 'd41d8cd98f00b204e9800998ecf8427e',
             'size': 0}
        ]
        patched_get_doc.assert_called_with('samples', where={'sample_id': 'sample1'})
        patched_patch_entry.assert_called_with(
            'samples',
            element_id='sample1',
            id_field='sample_id',
            update_lists=['files_delivered'],
            payload={'files_delivered': [
                {'file_path': 'project1/date_delivery/sample1/sample1.bam', 'md5': 'd41d8cd98f00b204e9800998ecf8427e',
                 'size': 0}
            ]}
        )

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    def test_add_file_downloaded(self, patched_get_doc):
        date_download = datetime.datetime.now()
        self.sample.add_file_downloaded('path/to/file.bam', 'testuser', date_download, 1024)
        expected_files_downloaded = [
            {'date': date_download.strftime('%d_%m_%Y_%H:%M:%S'), 'user': 'testuser', 'file_path': 'path/to/file.bam',
             'size': 1024}
        ]
        assert self.sample.list_file_downloaded == expected_files_downloaded

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    def test_add_file_downloaded_starting_with_project(self, patched_get_doc):
        date_download = datetime.datetime.now()
        self.sample.add_file_downloaded('path/to/file.bam', 'testuser', date_download, 1024)
        expected_files_downloaded = [
            {'date': date_download.strftime('%d_%m_%Y_%H:%M:%S'), 'user': 'testuser', 'file_path': 'path/to/file.bam',
             'size': 1024}
        ]
        assert self.sample.list_file_downloaded == expected_files_downloaded

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    @patch('bin.confirm_delivery.patch_entry')
    def test_update_list_file_downloaded(self, patched_patch_entry, patched_get_doc):
        date_download = datetime.datetime.now()
        self.sample.add_file_downloaded('project1/path/to/file.bam', 'testuser', date_download, 1024)

        self.sample.update_list_file_downloaded()

        patched_patch_entry.assert_called_with(
            'samples',
            element_id='sample1',
            id_field='sample_id',
            update_lists=['files_downloaded'],
            payload={'files_downloaded': [
                {'file_path': 'project1/path/to/file.bam', 'user': 'testuser',
                 'date': date_download.strftime('%d_%m_%Y_%H:%M:%S'), 'size': 1024}
            ]})

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    def test_file_missing(self, patched_get_doc):
        missing_files = ['path/to/file.bam', 'path/to/file.g.vcf.gz']
        assert self.sample.files_missing() == missing_files

        date_download = datetime.datetime.now()
        self.sample.add_file_downloaded('path/to/file.bam', 'testuser', date_download, 1024)

        missing_files = ['path/to/file.g.vcf.gz']
        assert self.sample.files_missing() == missing_files

        self.sample.add_file_downloaded('path/to/file.g.vcf.gz', 'testuser', date_download, 1024)
        assert self.sample.files_missing() == []

    def test_is_download_complete(self):
        with patch.object(DeliveredSample, 'files_missing', return_value=['file1']):
            assert not self.sample.is_download_complete()
        with patch.object(DeliveredSample, 'files_missing', return_value=[]):
            assert self.sample.is_download_complete()


class TestConfirmDelivery(TestProjectManagement):
    def setUp(self):
        cfg.load_config_file(os.path.join(self.root_path, 'etc', 'example_data_delivery.yaml'))
        self.c = ConfirmDelivery()

    def test_parse_aspera_reports(self):
        aspera_report = os.path.join(self.assets_path, 'confirm_delivery', 'filesreport_test.csv')
        file_list = parse_aspera_reports(aspera_report)
        assert len(file_list) == 31

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    def test_read_aspera_report(self, patched_get_doc):
        aspera_report = os.path.join(self.assets_path, 'confirm_delivery', 'filesreport_test.csv')

        self.c.read_aspera_report(aspera_report)
        assert len(self.c.samples_delivered) == 2

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    def test_test_sample_false(self, patched_get_doc):
        self.c.info = Mock()
        assert not self.c.test_sample('sample1')
        assert self.c.info.call_count == 3

    @patch('bin.confirm_delivery.get_document', return_value=sample3)
    def test_test_sample_true(self, patched_get_doc):
        assert self.c.test_sample('sample3')

    @patch('egcg_core.clarity.connection')
    @patch('egcg_core.clarity.get_workflow_stage')
    @patch('egcg_core.clarity.get_list_of_samples')
    def test_confirm_download_in_lims(self, mocked_get_list_of_samples, mocked_get_workflow_stage,
                                      mocked_lims_connection):
        mocked_get_list_of_samples.return_value = [Mock(artifact=Mock(spec=Artifact))]
        mocked_get_workflow_stage.return_value = Mock(step=Mock(spec=ProtocolStep, id='s1', permitted_containers=list()))
        self.c.confirmed_samples.append('sample1')
        self.c.confirm_download_in_lims()
        mocked_get_list_of_samples.assert_called_with(sample_names=['sample1'])
        mocked_get_workflow_stage.assert_called_with(stage_name='Download Confirmation EG 1.0 ST',
                                                     workflow_name='PostSeqLab EG 1.0 WF')
