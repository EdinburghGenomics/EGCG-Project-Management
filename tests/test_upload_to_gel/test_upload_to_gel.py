import hashlib
import os
import shutil
from unittest.mock import patch, PropertyMock, Mock

import pytest
import time
from egcg_core.config import cfg
from egcg_core.constants import ELEMENT_SAMPLE_EXTERNAL_ID

from tests import TestProjectManagement
from upload_to_gel.deliver_data_to_gel import send_action_to_rest_api, GelDataDelivery, DeliveryDB

patched_response = patch(
        'requests.request',
        return_value=Mock(status_code=200, content='')
    )

sample1 = {'project_id': 'project1', 'sample_id': 'sample1', 'user_sample_id': '123456789_ext_sample1'}
sample2 = {'project_id': 'project1', 'sample_id': 'sample2', 'user_sample_id': '223456789_ext_sample2'}
sample3 = {'project_id': 'project1', 'sample_id': 'sample3', 'user_sample_id': '323456789_ext_sample3'}
samples = {
    'sample1': sample1,
    'sample2': sample2,
    'sample3': sample3
}
fluidx = {
    'sample1': 'FD1',
    'sample2': 'FD2',
    'sample3': 'FD3'
}


def fake_get_sample(self, sample_id):
    return samples[sample_id]


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    with open(fname + '.md5', "w") as f:
        f.write(hash_md5.hexdigest() + "  " + fname)


class TestDeliveryDB(TestProjectManagement):
    def setUp(self):
        os.chdir(self.root_path)
        etc_config = os.path.join(self.root_path, 'etc', 'example_gel_data_delivery.yaml')
        cfg.load_config_file(etc_config)
        self.db_file = cfg.query('gel_upload', 'delivery_db')
        self.deliverydb = DeliveryDB()

    def tearDown(self):
        os.remove(self.db_file)

    def test_create(self):
        assert os.path.exists(self.db_file)

    def test_create_delivery(self):
        _id = self.deliverydb.create_delivery('sample1', 'external_sample1')
        assert _id == 'ED00000001'
        assert self.deliverydb.get_info_from(_id, 'sample_id') == ('sample1',)

    def test_get_info_from(self):
        obs = self.deliverydb.get_info_from('ED0', 'sample_id')
        assert obs is None
        _id = self.deliverydb.create_delivery('a_sample', 'an_external_sample')
        self.deliverydb.set_upload_state(_id, 'success')
        self.deliverydb.set_md5_state(_id, 'success')
        self.deliverydb.set_qc_state(_id, 'failed')
        self.deliverydb.delivery_db.commit()

        obs = self.deliverydb.get_info_from(_id, 'sample_id', 'upload_state', 'md5_state', 'qc_state')
        assert obs == ('a_sample', 'success', 'success', 'failed')

    def test_get_most_recent_delivery_id(self):
        id1 = self.deliverydb.create_delivery('sample1', 'external_sample1')
        time.sleep(1)
        id2 = self.deliverydb.create_delivery('sample1', 'external_sample1')
        id3 = self.deliverydb.create_delivery('sample2', 'external_sample2')
        id4 = self.deliverydb.create_delivery('sample3', 'external_sample3')

        assert self.deliverydb.get_most_recent_delivery_id('sample1') == id2
        assert self.deliverydb.get_most_recent_delivery_id('sample3') == id4

    def _test_set_state(self, set_func, *fields):
        did = self.deliverydb.create_delivery('sample1', 'external_sample1')
        assert self.deliverydb.get_info_from(did, *fields) == (None, None)
        set_func(did, 'pass')
        state, date = self.deliverydb.get_info_from(did, *fields)
        assert state == 'pass'
        assert date

    def test_set_upload_state(self):
        self._test_set_state(self.deliverydb.set_upload_state, 'upload_state', 'upload_confirm_date')

    def test_set_md5_state(self):
        self._test_set_state(self.deliverydb.set_md5_state, 'md5_state', 'md5_confirm_date')

    def test_set_qc_state(self):
        self._test_set_state(self.deliverydb.set_qc_state, 'qc_state', 'qc_confirm_date')


class TestGelDataDelivery(TestProjectManagement):
    patch_sample_data1 = patch.object(
        GelDataDelivery,
        'sample_data',
        new_callable=PropertyMock(return_value=sample1)
    )
    patch_fluidxbarcode1 = patch.object(
        GelDataDelivery,
        'fluidx_barcode',
        new_callable=PropertyMock(return_value='FD1')
    )
    patch_fluidxbarcode2 = patch.object(
        GelDataDelivery,
        'fluidx_barcode',
        new_callable=PropertyMock(return_value='FD2')
    )
    patch_create_delivery = patch.object(DeliveryDB, 'create_delivery', return_value='ED00000005')
    patch_send_action = patch('upload_to_gel.deliver_data_to_gel.send_action_to_rest_api')

    def __init__(self, *args, **kwargs):
        super(TestProjectManagement, self).__init__(*args, **kwargs)
        cfg.load_config_file(os.path.join(os.path.dirname(self.root_test_path), 'etc', 'example_gel_data_delivery.yaml'))
        os.chdir(os.path.dirname(self.root_test_path))
        self.assets_delivery = os.path.join(self.assets_path, 'data_delivery')

    def setUp(self):
        self.dest_proj1 = os.path.join(self.assets_delivery, 'dest', 'project1', '2017-01-01')
        os.makedirs(self.dest_proj1, exist_ok=True)
        self.staging_dir = os.path.join(self.assets_delivery, 'staging')

        for sample in ['sample1', 'sample2', 'sample3']:
            sample_dir = os.path.join(self.dest_proj1, fluidx.get(sample))
            os.makedirs(sample_dir, exist_ok=True)
            for suffix in ['_R1.fastq.gz', '_R2.fastq.gz']:
                f = os.path.join(sample_dir, samples.get(sample).get(ELEMENT_SAMPLE_EXTERNAL_ID) + suffix)
                open(f, 'w').close()
                md5(f)

        self.gel_data_delivery_dry = GelDataDelivery(
            'sample1',
            work_dir=self.staging_dir,
            dry_run=True,
            no_cleanup=True
        )
        self.gel_data_delivery = GelDataDelivery(
            'sample1',
            work_dir=self.staging_dir,
            dry_run=False,
            no_cleanup=True
        )

    def tearDown(self):
        db_file = cfg.query('gel_upload', 'delivery_db')
        if os.path.exists(db_file):
            os.remove(db_file)
        shutil.rmtree(os.path.dirname(self.dest_proj1))
        for d in os.listdir(self.staging_dir):
            fp = os.path.join(self.staging_dir, d)
            if os.path.isdir(fp):
                shutil.rmtree(fp)
            else:
                os.unlink(fp)

    def test_link_fastq_files(self):
        with self.patch_sample_data1, self.patch_fluidxbarcode1:
            self.gel_data_delivery_dry.link_fastq_files(self.staging_dir)
            assert os.path.islink(os.path.join(self.staging_dir, sample1.get('user_sample_id') + '_R1.fastq.gz'))
            assert os.path.islink(os.path.join(self.staging_dir, sample1.get('user_sample_id') + '_R2.fastq.gz'))

    def test_link_fastq_files_fail(self):
        with self.patch_sample_data1, self.patch_fluidxbarcode2:
            # Mixing sample 1 and sample 2 id won't work
            with pytest.raises(FileNotFoundError):
                self.gel_data_delivery_dry.link_fastq_files(
                    self.staging_dir,
                )

    @patch('upload_to_gel.deliver_data_to_gel.DeliveryAPIClient')
    def test_send_action_to_rest_api(self, mocked_request):
        send_action_to_rest_api('create', delivery_id='ED0000000001', sample_id='sample1')
        mocked_request.assert_any_call(
            action='create',
            delivery_id='ED0000000001',
            host='restapi.gelupload.com',
            pswd='passwd',
            sample_id='sample1',
            user='restuser'
        )

    def test_delivery_id(self):
        with self.patch_create_delivery, self.patch_sample_data1:
            assert self.gel_data_delivery.delivery_id == 'ED00000005'

    def _check_already_uploaded(self):
        with patch.object(GelDataDelivery, 'info') as mocked_info:
            self.gel_data_delivery_dry.deliver_data()
            assert mocked_info.call_count == 1
            mocked_info.assert_called_with('Already uploaded successfully: will do nothing without --force_new_delivery')

    def _check_dry_run(self):
        with patch.object(GelDataDelivery, 'info') as mocked_info:
            self.gel_data_delivery_dry.deliver_data()
            assert mocked_info.call_count == 1
            mocked_info.assert_called_with(
                'Dry run: will do delivery %s for sample %s, barcode %s',
                'ED00TEST', 'sample1', '123456789_ext_sample1'
            )

    @patch.object(DeliveryDB, 'get_info_from', return_value=('passed',))
    def test_deliver_data_dry(self, mocked_get_info):
        with self.patch_sample_data1, self.patch_fluidxbarcode1:
            self._check_already_uploaded()
            mocked_get_info.return_value = ('failed',)
            self._check_dry_run()

    def test_force(self):
        with self.patch_sample_data1, self.patch_fluidxbarcode1, \
             patch.object(DeliveryDB, 'get_info_from', return_value=('passed',)):
            self._check_already_uploaded()
            self.gel_data_delivery_dry.force_new_delivery = True
            self._check_dry_run()

    def _test_deliver_data(self, execute_return, delivered_state):
        with self.patch_sample_data1, self.patch_fluidxbarcode1, \
             self.patch_send_action as mocked_send_action, \
             patch('egcg_core.executor.local_execute') as mock_execute, \
             patch.object(GelDataDelivery, 'delivery_id_exists', return_value=False):
            mock_execute.return_value = Mock(join=Mock(return_value=execute_return))
            self.gel_data_delivery.deliver_data()
            source = os.path.join(self.gel_data_delivery.staging_dir, self.gel_data_delivery.delivery_id)

            rsync_cmd = ('rsync -rv -L --timeout=300 --append --partial --chmod ug+rwx,o-rwx --perms ',
                         '-e "ssh -o StrictHostKeyChecking=no -o TCPKeepAlive=yes -o ServerAliveInterval=100 ',
                         '-o KeepAlive=yes -o BatchMode=yes -o LogLevel=Error -i path/to/id_rsa.pub -p 22" ',
                         '{source} user@gelupload.com:/destination/'.format(source=source))
            mock_execute.assert_any_call(''.join(rsync_cmd))

            assert os.listdir(source) == [self.gel_data_delivery.external_id]
            assert os.listdir(source + '/' + self.gel_data_delivery.external_id) == ['fastq', 'md5sum.txt']

            # Check API creation
            mocked_send_action.assert_any_call(
                action='create',
                delivery_id=self.gel_data_delivery.delivery_id,
                sample_id='123456789_ext_sample1'
            )
            # Check database state
            assert (delivered_state,) == self.gel_data_delivery.deliver_db.get_info_from(
                self.gel_data_delivery.delivery_id, 'upload_state'
            )
            # Check API delivered state and how many time rsync was called
            if delivered_state == 'passed':
                mocked_send_action.assert_called_with(
                    action='delivered',
                    delivery_id=self.gel_data_delivery.delivery_id,
                    sample_id='123456789_ext_sample1'
                )
                assert mock_execute.call_count == 1
            elif delivered_state == 'failed':
                mocked_send_action.assert_called_with(
                    action='upload_failed',
                    delivery_id=self.gel_data_delivery.delivery_id,
                    sample_id='123456789_ext_sample1',
                    failure_reason='rsync returned %s exit code' % execute_return
                )
                assert mock_execute.call_count == 3

    def test_deliver_data_success(self):
        self._test_deliver_data(execute_return=0, delivered_state='passed')

    def test_deliver_data_fail(self):
        self._test_deliver_data(execute_return=10, delivered_state='failed')

    def _test_check_delivery(self, api_return_state, expected_md5, expected_qc):
        _id = self.gel_data_delivery.deliver_db.create_delivery('sample1', '123456789_ext_sample1')
        self.gel_data_delivery.deliver_db.set_upload_state(_id, 'passed')

        with self.patch_sample_data1, self.patch_send_action as mocked_send_action:
            mocked_send_action.return_value = Mock(json=Mock(return_value={'state': api_return_state}))
            self.gel_data_delivery.check_delivery_data()

            self.gel_data_delivery.deliver_db.cursor.execute('select * from delivery;')
            obs = self.gel_data_delivery.deliver_db.cursor.fetchone()
            assert obs[6] == expected_md5  # md5_status
            assert obs[8] == expected_qc  # qc_status

    def test_check_delivery_passed(self):
        self._test_check_delivery('qc_passed', 'passed', 'passed')

    def test_check_delivery_md5_passed(self):
        self._test_check_delivery('md5_passed', 'passed', None)

    def test_check_delivery_md5_failed(self):
        self._test_check_delivery('md5_failed', 'failed', None)

    def test_check_delivery_qc_failed(self):
        self._test_check_delivery('qc_failed', 'passed', 'failed')

    def test_no_check_delivery(self):
        _id = self.gel_data_delivery.deliver_db.create_delivery('sample1', '123456789_ext_sample1')
        self.gel_data_delivery.deliver_db.set_upload_state(_id, 'passed')
        self.gel_data_delivery.deliver_db.set_md5_state(_id, 'passed')
        self.gel_data_delivery.deliver_db.set_qc_state(_id, 'passed')

        with patch.object(GelDataDelivery, 'error') as mock_error:
            self.gel_data_delivery.check_delivery_data()

            self.gel_data_delivery.deliver_db.cursor.execute('select * from delivery;')
            obs = self.gel_data_delivery.deliver_db.cursor.fetchone()
            mock_error.assert_called_with(
                'Delivery %s sample %s qc check failed - was checked before on %s',
                'ED00000001',
                'sample1',
                obs[9]
            )
