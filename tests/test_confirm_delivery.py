import os
from unittest.mock import patch

from os.path import join

import shutil
from egcg_core.config import cfg

from bin.confirm_delivery import parse_aspera_reports, DeliveredSample, ConfirmDelivery
from tests import TestProjectManagement


sample1 = {
    'sample_id': 'sample1',
    'files_delivered': [
        {'file_path': 'path/to/file.bam', 'md5': 'md5stringforbam'},
        {'file_path': 'path/to/file.g.vcf.gz', 'md5': 'md5stringforvcf'}
    ]
}

sample2 = {
    'sample_id': 'sample1',
    'project_id': 'project1',
    'files_delivered': []
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

    @patch('bin.confirm_delivery.get_document', return_value=sample1 )
    def test_data(self, patched_get_doc):
        data = self.sample.data
        assert data['sample_id'] == 'sample1'
        patched_get_doc.assert_called_with('samples', where={'sample_id': 'sample1'})

    @patch('bin.confirm_delivery.patch_entry')
    def test_upload_list_file_delivered(self, patched_get_patch_entry):
        self.sample.delivery_dir = join(self.assets_path, 'data_delivery', 'source')
        list_files = [
            join(self.assets_path, 'data_delivery', 'source', 'test_project', 'deliverable_sample', 'user_s_id.bam'),
            join(self.assets_path, 'data_delivery', 'source', 'test_project', 'deliverable_sample', 'user_s_id.g.vcf.gz'),
        ]

        self.sample.upload_list_file_delivered(list_files)
        patched_get_patch_entry.assert_called_with(
            'samples',
            element_id='sample1',
            id_field='sample_id',
            update_lists = ['files_delivered'],
            payload={'files_delivered': [
                {'file_path': 'test_project/deliverable_sample/user_s_id.bam', 'md5': 'd41d8cd98f00b204e9800998ecf8427e'},
                {'file_path': 'test_project/deliverable_sample/user_s_id.g.vcf.gz', 'md5': 'd41d8cd98f00b204e9800998ecf8427e'}
            ]}
        )

    @patch('bin.confirm_delivery.get_document', return_value=sample1)
    def test_list_file_delivered_from_data(self, patched_get_doc):
        files_delivered = self.sample.list_file_delivered
        assert files_delivered == sample1.get('files_delivered')
        patched_get_doc.assert_called_with('samples', where={'sample_id': 'sample1'})


    @patch('bin.confirm_delivery.get_document', return_value=sample2)
    @patch('bin.confirm_delivery.patch_entry')
    def test_list_file_delivered_no_data(self, patched_get_patch_entry, patched_get_doc):
        files_delivered = self.sample.list_file_delivered
        assert files_delivered == [
            {'file_path': 'project1/date_delivery/sample1/sample1.bam', 'md5': 'd41d8cd98f00b204e9800998ecf8427e'}
        ]
        patched_get_doc.assert_called_with('samples', where={'sample_id': 'sample1'})
        patched_get_patch_entry.assert_called_with(
            'samples',
            element_id='sample1',
            id_field='sample_id',
            update_lists=['files_delivered'],
            payload={'files_delivered': [
                {'file_path': 'project1/date_delivery/sample1/sample1.bam', 'md5': 'd41d8cd98f00b204e9800998ecf8427e'}
            ]}
        )

class TestConfirmDelivery(TestProjectManagement):

    def setUp(self):
        cfg.load_config_file(os.path.join(self.root_path, 'etc', 'example_data_delivery.yaml'))
        self.c = ConfirmDelivery()

    def test_parse_aspera_reports(self):
        aspera_report = os.path.join(self.assets_path, 'confirm_delivery', 'filesreport_test.csv')
        file_list = parse_aspera_reports(aspera_report)
        assert len(file_list) == 421

    def test_read_aspera_report(self):
        aspera_report = os.path.join(self.assets_path, 'confirm_delivery', 'filesreport_test.csv')

        self.c.read_aspera_report(aspera_report)
