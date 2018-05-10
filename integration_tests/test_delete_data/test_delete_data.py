import os
from unittest.mock import patch
from integration_tests import setup_delivered_samples, IntegrationTest
from egcg_core import rest_communication, archive_management
from egcg_core.config import cfg
from data_deletion import client


class TestDeletion(IntegrationTest):
    patches = (
        patch('data_deletion.client.load_config'),
    )

    def _run_main(self, argv):
        client.main(argv + ['--work_dir', self.run_dir])


class TestDeleteRawData(TestDeletion):
    def setUp(self):
        super().setUp()
        self.raw_dir = os.path.join(self.run_dir, 'raw')
        self.archive_dir = os.path.join(self.run_dir, 'archives')

        cfg.content = {
            'executor': self.cfg['executor'],
            'data_deletion': {
                'raw_data': self.raw_dir,
                'raw_archives': self.archive_dir
            }
        }

        for d in ('Data', 'Logs', 'Thumbnail_Images', 'some_metadata'):
            subdir = os.path.join(self.raw_dir, 'a_run', d)
            os.makedirs(subdir, exist_ok=True)
            open(os.path.join(subdir, 'some_data.txt'), 'w').close()

        os.makedirs(self.archive_dir, exist_ok=True)

        rest_communication.post_entry(
            'run_elements',
            {
                'run_element_id': 'a_run_element', 'run_id': 'a_run', 'lane': 1, 'project_id': 'a_project',
                'library_id': 'a_library', 'sample_id': 'a_sample', 'reviewed': 'pass'
            }
        )
        rest_communication.post_entry(
            'analysis_driver_procs',
            {'proc_id': 'a_proc', 'dataset_type': 'run', 'dataset_name': 'a_run', 'status': 'finished'}
        )
        rest_communication.post_entry('runs', {'run_id': 'a_run', 'analysis_driver_procs': ['a_proc']})

    def test_raw_data(self):
        run_dir = os.path.join(self.raw_dir, 'a_run')
        self.assertTrue('run dir exists', os.path.isdir(run_dir))

        self._run_main(['raw'])

        self.assertFalse('run dir deleted', os.path.isdir(run_dir))
        self.assertTrue(
            'metadata archived',
            os.path.isfile(os.path.join(self.archive_dir, 'a_run', 'some_metadata', 'some_data.txt'))
        )

    def test_unreviewed_raw_data(self):
        rest_communication.patch_entry('run_elements', {'reviewed': 'not reviewed'}, 'run_element_id', 'a_run_element')
        run_dir = os.path.join(self.raw_dir, 'a_run')
        self.assertTrue('run dir exists', os.path.isdir(run_dir))

        self._run_main(['raw'])

        # nothing should have happened
        self.assertTrue('run dir not deleted', os.path.isdir(run_dir))
        self.assertFalse('metadata not archived', os.path.isdir(os.path.join(self.archive_dir, 'a_run')))


class TestDeleteDeliveredData(TestDeletion):
    def setUp(self):
        super().setUp()

        self.fastq_dir = os.path.join(self.run_dir, 'fastqs')
        self.fastq_archive_dir = os.path.join(self.run_dir, 'fastq_archives')
        self.processed_data_dir = os.path.join(self.run_dir, 'processed_data')
        self.processed_archive_dir = os.path.join(self.run_dir, 'processed_archives')
        self.delivered_data_dir = os.path.join(self.run_dir, 'delivered_data')

        cfg.content = {
            'executor': self.cfg['executor'],
            'data_deletion': {
                'fastqs': self.fastq_dir,
                'fastq_archives': self.fastq_archive_dir,
                'processed_data': self.processed_data_dir,
                'processed_archives': self.processed_archive_dir,
                'delivered_data': self.delivered_data_dir
            }
        }
        self.all_files = setup_delivered_samples(self.processed_data_dir, self.delivered_data_dir, self.fastq_dir)

    def test_manual_release(self):
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            self.assertTrue('%s files archived' % sample_id, all(archive_management.is_archived(f) for f in self.all_files[sample_id]))
            self.assertEqual('%s no data deleted' % sample_id, rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'], 'none')

        self._run_main(
            ['delivered_data', '--manual_delete', 'sample_1', 'sample_2', 'sample_3',
             '--sample_ids', 'sample_1', 'sample_2']
        )
        for sample_id in ('sample_1', 'sample_2'):
            statuses = {f: archive_management.archive_states(f) for f in self.all_files[sample_id]}
            self.assertTrue('%s files released' % sample_id, all(archive_management.is_released(f) for f in self.all_files[sample_id]), statuses)
            self.assertEqual('%s marked as released', rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'], 'on lustre')

        # sample 3 should not be released
        statuses = {f: archive_management.archive_states(f) for f in self.all_files['sample_3']}
        self.assertTrue('sample_3 files not released', all(archive_management.is_archived(f) and not archive_management.is_released(f) for f in self.all_files['sample_3']), statuses)
        self.assertEqual('sample_3 not marked as released', rest_communication.get_document('samples', where={'sample_id': 'sample_3'})['data_deleted'], 'none')

    def test_dry_run(self):
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            self.assertTrue('%s files archived' % sample_id, all(archive_management.is_archived(f) for f in self.all_files[sample_id]))
            self.assertEqual('%s no data deleted', rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'], 'none')

        self._run_main(
            ['delivered_data', '--manual_delete', 'sample_1', 'sample_2', 'sample_3',
             '--sample_ids', 'sample_1', 'sample_2', '--dry_run']
        )

        # nothing should have happened
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            self.assertTrue('%s files still archived', all(archive_management.is_archived(f) for f in self.all_files[sample_id]))
            self.assertEqual('%s still no data deleted', rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'], 'none')
