import os
from time import sleep
from shutil import rmtree
from collections import defaultdict
from unittest.mock import patch
from contextlib import contextmanager
from integration_tests import IntegrationTest, integration_cfg
from egcg_core import rest_communication, archive_management
from egcg_core.config import cfg
from data_deletion import client

work_dir = os.path.dirname(__file__)


@contextmanager
def patches():
    _patches = []

    def _patch(ppath, **kwargs):
        _p = patch(ppath, **kwargs)
        _p.start()
        _patches.append(_p)

    _patch('data_deletion.client.load_config')

    yield

    for p in _patches:
        p.stop()


class TestDeletion(IntegrationTest):
    @staticmethod
    def _run_main(argv):
        with patches():
            client.main(argv + ['--work_dir', work_dir])


class TestDeleteRawData(TestDeletion):
    raw_dir = os.path.join(work_dir, 'raw')
    archive_dir = os.path.join(work_dir, 'archives')

    @classmethod
    def setUpClass(cls):
        cfg.content = {
            'executor': integration_cfg['executor'],
            'data_deletion': {
                'raw_data': cls.raw_dir,
                'raw_archives': cls.archive_dir
            }
        }

    def setUp(self):
        super().setUp()

        for d in ('Data', 'Logs', 'Thumbnail_Images', 'some_metadata'):
            subdir = os.path.join(self.raw_dir, 'a_run', d)
            os.makedirs(subdir, exist_ok=True)
            open(os.path.join(subdir, 'some_data.txt'), 'w').close()

        os.makedirs(self.archive_dir, exist_ok=True)
        for x in os.listdir(self.archive_dir):
            rmtree(os.path.join(self.archive_dir, x))

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
        rest_communication.post_entry(
            'runs',
            {'run_id': 'a_run', 'analysis_driver_procs': ['a_proc']}
        )

    def test_raw_data(self):
        run_dir = os.path.join(self.raw_dir, 'a_run')
        assert os.path.isdir(run_dir)

        with patches():
            self._run_main(['raw'])

        assert not os.path.isdir(run_dir)
        assert os.path.isfile(os.path.join(self.archive_dir, 'a_run', 'some_metadata', 'some_data.txt'))

    def test_unreviewed_raw_data(self):
        rest_communication.patch_entry('run_elements', {'reviewed': 'not reviewed'}, 'run_element_id', 'a_run_element')
        run_dir = os.path.join(self.raw_dir, 'a_run')
        assert os.path.isdir(run_dir)

        with patches():
            self._run_main(['raw'])

        # nothing should have happened
        assert os.path.isdir(run_dir)
        assert not os.path.isdir(os.path.join(self.archive_dir, 'a_run'))


class TestDeleteDeliveredData(TestDeletion):
    fastq_dir = os.path.join(work_dir, 'fastqs')
    fastq_archive_dir = os.path.join(work_dir, 'fastq_archives')
    processed_data_dir = os.path.join(work_dir, 'processed_data')
    processed_archive_dir = os.path.join(work_dir, 'processed_archives')
    delivered_data_dir = os.path.join(work_dir, 'delivered_data')

    @classmethod
    def setUpClass(cls):
        cfg.content = {
            'executor': integration_cfg['executor'],
            'data_deletion': {
                'fastqs': cls.fastq_dir,
                'fastq_archives': cls.fastq_archive_dir,
                'processed_data': cls.processed_data_dir,
                'processed_archives': cls.processed_archive_dir,
                'delivered_data': cls.delivered_data_dir
            }
        }

    def setUp(self):
        super().setUp()

        for d in (self.processed_data_dir, self.delivered_data_dir, self.fastq_dir):
            if os.path.isdir(d):
                rmtree(d)

        self.all_files = defaultdict(list)
        for i in range(1, 4):
            sample_id = 'sample_' + str(i)
            ext_sample_id = 'ext_' + sample_id
            sample_dir = os.path.join(self.processed_data_dir, 'a_project', sample_id)
            delivered_dir = os.path.join(self.delivered_data_dir, 'a_project', 'a_delivery_date', sample_id)
            fastq_dir = os.path.join(self.fastq_dir, 'a_run', 'a_project', sample_id)

            os.makedirs(sample_dir)
            os.makedirs(fastq_dir)
            os.makedirs(delivered_dir)

            rest_communication.post_entry(
                'samples',
                {'sample_id': sample_id, 'user_sample_id': ext_sample_id, 'project_id': 'a_project'}
            )
            rest_communication.post_entry(
                'run_elements',
                {'run_element_id': 'a_run_%s_ATGC' % i, 'run_id': 'a_run', 'lane': i, 'barcode': 'ATGC',
                 'project_id': 'a_project', 'sample_id': sample_id, 'library_id': 'a_library'}
            )

            for ext in ('.bam', '.vcf.gz'):
                f = os.path.join(sample_dir, ext_sample_id + ext)
                self.all_files[sample_id].append(f)

            for r in ('1', '2'):
                f = os.path.join(fastq_dir, 'L00%s_R%s.fastq.gz' % (i, r))
                self.all_files[sample_id].append(f)

            for f in self.all_files[sample_id]:
                open(f, 'w').close()
                os.link(f, os.path.join(delivered_dir, os.path.basename(f)))
                archive_management.register_for_archiving(f)

        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            for f in self.all_files[sample_id]:
                while not archive_management.is_archived(f):
                    sleep(10)

    def test_manual_release(self):
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            assert all(archive_management.is_archived(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'none'

        self._run_main(
            ['delivered_data', '--manual_delete', 'sample_1', 'sample_2', 'sample_3',
             '--sample_ids', 'sample_1', 'sample_2']
        )
        for sample_id in ('sample_1', 'sample_2'):
            assert all(archive_management.is_released(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'on lustre'

        # sample 3 should not be released
        assert all(archive_management.is_archived(f) and not archive_management.is_released(f) for f in self.all_files['sample_3'])
        assert rest_communication.get_document('samples', where={'sample_id': 'sample_3'})['data_deleted'] == 'none'

    def test_dry_run(self):
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            assert all(archive_management.is_archived(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'none'

        self._run_main(
            ['delivered_data', '--manual_delete', 'sample_1', 'sample_2', 'sample_3',
             '--sample_ids', 'sample_1', 'sample_2', '--dry_run']
        )

        # nothing should have happened
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            assert all(archive_management.is_archived(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'none'
