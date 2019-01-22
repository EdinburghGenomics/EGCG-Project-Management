import os
from shutil import rmtree
from datetime import timedelta
from unittest.mock import Mock, patch
from integration_tests import IntegrationTest, integration_cfg, setup_delivered_samples, \
    setup_samples_deleted_from_tier1
from egcg_core import rest_communication, archive_management
from egcg_core.config import cfg
from data_deletion import client
from data_deletion.raw_data import RawDataDeleter, reporting_app_date_format

work_dir = os.path.dirname(__file__)


def fake_get_sample(sample_id):
    m = Mock(udf={})
    if sample_id == 'sample_4':
        m.udf['2D Barcode'] = 'sample_4_2d_barcode'

    return m


class TestDeletion(IntegrationTest):
    patches = (
        patch('data_deletion.client.load_config'),
        patch('data_deletion.clarity.get_sample', new=fake_get_sample)
    )

    @staticmethod
    def _run_main(argv):
        client.main(argv + ['--work_dir', work_dir])


class TestDeleteRawData(TestDeletion):
    raw_dir = os.path.join(work_dir, 'raw')
    raw_run_dir = os.path.join(raw_dir, 'a_run')
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
            subdir = os.path.join(self.raw_run_dir, d)
            os.makedirs(subdir, exist_ok=True)
            open(os.path.join(subdir, 'some_data.txt'), 'w').close()

        os.makedirs(self.archive_dir, exist_ok=True)
        for x in os.listdir(self.archive_dir):
            rmtree(os.path.join(self.archive_dir, x))

        deletion_threshold = RawDataDeleter._now() - timedelta(days=15)
        rest_communication.post_entry(
            'run_elements',
            {
                'run_element_id': 'a_run_element', 'run_id': 'a_run', 'lane': 1, 'project_id': 'a_project',
                'library_id': 'a_library', 'barcode': 'ATGCATGC', 'sample_id': 'a_sample', 'reviewed': 'pass',
                'useable': 'yes', 'useable_date': deletion_threshold.strftime(reporting_app_date_format)
            }
        )
        # unknown barcode has no useable date, but should not prevent a run from being deletable
        rest_communication.post_entry(
            'run_elements',
            {
                'run_element_id': 'an_unknown_barcode', 'run_id': 'a_run', 'lane': 1, 'project_id': 'a_project',
                'library_id': 'a_library', 'barcode': 'unknown', 'sample_id': 'a_sample'
            }
        )
        rest_communication.post_entry(
            'analysis_driver_procs',
            {'proc_id': 'a_proc', 'dataset_type': 'run', 'dataset_name': 'a_run', 'status': 'finished'}
        )
        rest_communication.post_entry('runs', {'run_id': 'a_run', 'analysis_driver_procs': ['a_proc']})
        self._assert_deletion_not_occurred()

    def test_delete(self):
        self._run_main(['raw'])
        self._assert_deletion_occurred()

    def test_review_status(self):
        rest_communication.patch_entry('run_elements', {'reviewed': 'not reviewed'}, 'run_element_id', 'a_run_element')
        self._run_main(['raw'])
        self._assert_deletion_not_occurred()

    def test_proc_status(self):
        rest_communication.patch_entry('analysis_driver_procs', {'status': 'processing'}, 'proc_id', 'a_proc')
        self._run_main(['raw'])
        self._assert_deletion_not_occurred()

    def test_deletion_age(self):
        deletion_threshold = RawDataDeleter._now() - timedelta(days=10)
        rest_communication.patch_entry(
            'run_elements',
            {'useable_date': deletion_threshold.strftime(reporting_app_date_format)},
            'run_element_id',
            'a_run_element'
        )
        self._run_main(['raw'])
        self._assert_deletion_not_occurred()

    def test_manual_delete(self):
        rest_communication.patch_entry('run_elements', {'reviewed': 'not reviewed'}, 'run_element_id', 'a_run_element')
        self._run_main(['raw', '--manual_delete', 'a_run'])
        self._assert_deletion_occurred()

    def _assert_deletion_not_occurred(self):
        assert os.path.isdir(self.raw_run_dir)
        assert not os.path.isdir(os.path.join(self.archive_dir, 'a_run'))

    def _assert_deletion_occurred(self):
        assert not os.path.isdir(self.raw_run_dir)
        assert os.path.isfile(os.path.join(self.archive_dir, 'a_run', 'some_metadata', 'some_data.txt'))


class TestDeleteDeliveredData(TestDeletion):
    fastq_dir = os.path.join(work_dir, 'fastqs')
    fastq_archive_dir = os.path.join(work_dir, 'fastq_archives')
    processed_data_dir = os.path.join(work_dir, 'processed_data')
    processed_archive_dir = os.path.join(work_dir, 'processed_archives')
    delivered_data_dir = os.path.join(work_dir, 'delivered_data')

    patches = TestDeletion.patches + (
        patch('data_deletion.clarity.get_sample_release_date', return_value='a_release_date'),
    )

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
        self.all_files = setup_delivered_samples(self.processed_data_dir, self.delivered_data_dir, self.fastq_dir)

    def test_manual_release(self):
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            assert all(archive_management.is_archived(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'none'

        self._run_main(
            ['delivered_data', '--manual_delete', 'sample_1', 'sample_2', 'sample_3',
             '--sample_ids', 'sample_1', 'sample_2']
        )
        for sample_id in ('sample_1', 'sample_2'):
            statuses = {f: archive_management.archive_states(f) for f in self.all_files[sample_id]}

            assert all(archive_management.is_released(f) for f in self.all_files[sample_id]), statuses
            assert not self._delivered_files_exist(sample_id)
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'on lustre'

        # sample 3 should not be released
        statuses = {f: archive_management.archive_states(f) for f in self.all_files['sample_3']}
        assert all(archive_management.is_archived(f) and not archive_management.is_released(f) for f in self.all_files['sample_3']), statuses
        assert self._delivered_files_exist('sample_3')
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

    def test_fluidx_deletion(self):
        assert all(archive_management.is_archived(f) for f in self.all_files['sample_4'])
        assert rest_communication.get_document('samples', where={'sample_id': 'sample_4'})['data_deleted'] == 'none'

        self._run_main(['delivered_data', '--manual_delete', 'sample_4', '--sample_ids', 'sample_4'])

        assert all(archive_management.is_released(f) for f in self.all_files['sample_4'])
        assert not self._delivered_files_exist('sample_4', 'sample_4_2d_barcode')
        assert rest_communication.get_document('samples', where={'sample_id': 'sample_4'})['data_deleted'] == 'on lustre'

    def _delivered_files_exist(self, sample_id, fluidx_barcode=None):
        basenames = sorted(os.path.basename(f) for f in self.all_files[sample_id])
        delivered_dir = os.path.join(self.delivered_data_dir, 'a_project', 'a_delivery_date', fluidx_barcode or sample_id)
        if not os.path.isdir(delivered_dir):
            return False

        delivered_files = sorted(os.listdir(delivered_dir))
        if not delivered_files:
            return False

        elif delivered_files == basenames:
            return True

        raise ValueError('Incomplete delivered files: %s' % delivered_files)


class TestDeleteFinalData(TestDeletion):
    fastq_dir = os.path.join(work_dir, 'fastqs')
    fastq_archive_dir = os.path.join(work_dir, 'fastq_archives')
    processed_data_dir = os.path.join(work_dir, 'processed_data')
    processed_archive_dir = os.path.join(work_dir, 'processed_archives')
    delivered_data_dir = os.path.join(work_dir, 'delivered_data')

    patches = TestDeletion.patches + (
        patch('data_deletion.clarity.get_sample_release_date', return_value='2016-12-01'),
    )

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
        self.all_files = setup_samples_deleted_from_tier1(
            self.processed_data_dir, self.delivered_data_dir, self.fastq_dir,
            self.processed_archive_dir, self.fastq_archive_dir
        )

    def test_manual_release(self):
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            assert all(archive_management.is_released(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})[
                       'data_deleted'] == 'on lustre'

        self._run_main(
            ['final_deletion', '--manual_delete', 'sample_1', 'sample_2', 'sample_3',
             '--sample_ids', 'sample_1', 'sample_2']
        )
        for sample_id in ('sample_1', 'sample_2'):
            assert all(not os.path.exists(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'all'

        # sample 3 should not be deleted
        assert all(archive_management.is_released(f) for f in self.all_files['sample_3'])
        assert rest_communication.get_document('samples', where={'sample_id': 'sample_3'})[
                   'data_deleted'] == 'on lustre'

        assert os.path.exists(os.path.join(self.processed_data_dir, 'a_project'))
        assert not os.path.exists(os.path.join(self.processed_archive_dir, 'a_project'))
        assert os.path.exists(os.path.join(self.fastq_dir, 'a_run'))
        assert not os.path.exists(os.path.join(self.fastq_archive_dir, 'a_run'))

    def test_manual_release_and_archiving(self):
        for sample_id in ('sample_1', 'sample_2', 'sample_3', 'sample_4'):
            assert all(archive_management.is_released(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})[
                       'data_deleted'] == 'on lustre'

        self._run_main(
            ['final_deletion', '--manual_delete', 'sample_1', 'sample_2', 'sample_3', 'sample_4']
        )
        for sample_id in ('sample_1', 'sample_2', 'sample_3', 'sample_4'):
            assert all(not os.path.exists(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'all'

        assert not os.path.exists(os.path.join(self.processed_data_dir, 'a_project'))
        assert os.path.exists(os.path.join(self.processed_archive_dir, 'a_project'))
        assert not os.path.exists(os.path.join(self.fastq_dir, 'a_run'))
        assert os.path.exists(os.path.join(self.fastq_archive_dir, 'a_run'))

    def test_dry_run(self):
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            assert all(archive_management.is_released(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'on lustre'

        self._run_main(
            ['delivered_data', '--manual_delete', 'sample_1', 'sample_2', 'sample_3',
             '--sample_ids', 'sample_1', 'sample_2', '--dry_run']
        )

        # nothing should have happened
        for sample_id in ('sample_1', 'sample_2', 'sample_3'):
            assert all(archive_management.is_released(f) for f in self.all_files[sample_id])
            assert rest_communication.get_document('samples', where={'sample_id': sample_id})['data_deleted'] == 'on lustre'
