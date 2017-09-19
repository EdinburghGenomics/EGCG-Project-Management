import hashlib
import os
import shutil
from unittest.mock import patch, PropertyMock, Mock

import pytest
from egcg_core.config import cfg
from egcg_core.constants import ELEMENT_SAMPLE_EXTERNAL_ID

from tests import TestProjectManagement
from upload_to_gel.deliver_data_to_gel import send_action_to_rest_api, GelDataDelivery, DeliveryDB

patched_response = patch(
        'requests.request',
        return_value=Mock(status_code=200, content='')
    )

sample1 = {'sample_id': 'sample1', 'user_sample_id': '123456789_ext_sample1'}
sample2 = {'sample_id': 'sample2', 'user_sample_id': '223456789_ext_sample2'}
sample3 = {'sample_id': 'sample3', 'user_sample_id': '323456789_ext_sample3'}
samples = {
    'sample1': sample1,
    'sample2': sample2,
    'sample3': sample3
}

def fake_get_sample(self, sample_id):
    return samples[sample_id]


def touch(f):
    open(f, 'w').close()


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
        id = self.deliverydb.create_delivery('sample1', 'external_sample1')
        assert id == 1
        assert self.deliverydb.get_sample_from(id) == 'sample1'

    def test_set_upload_date(self):
        id = self.deliverydb.create_delivery('sample1', 'external_sample1')
        info = self.deliverydb.get_info_from(id)
        assert info[4] is None
        self.deliverydb.set_upload_date(id)
        info = self.deliverydb.get_info_from(id)
        assert info[4] is not None

    def test_set_md5_date(self):
        id = self.deliverydb.create_delivery('sample1', 'external_sample1')
        info = self.deliverydb.get_info_from(id)
        assert info[5] is None
        self.deliverydb.set_md5_date(id)
        info = self.deliverydb.get_info_from(id)
        assert info[5] is not None


class TestGelDataDelivery(TestProjectManagement):

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
            sample_dir = os.path.join(self.dest_proj1, sample)
            os.makedirs(sample_dir, exist_ok=True)
            for suffix in ['_R1.fastq.gz', '_R2.fastq.gz', ]:
                f = os.path.join(sample_dir,samples.get(sample).get(ELEMENT_SAMPLE_EXTERNAL_ID) + suffix)
                touch(f)
                md5(f)
        self.gel_data_delivery_dry = GelDataDelivery(
            self.staging_dir,
            'project1',
            'sample1',
            dry_run=True,
            no_cleanup=True
        )
        self.gel_data_delivery = GelDataDelivery(
            self.staging_dir,
            'project1',
            'sample1',
            dry_run=False,
            no_cleanup=True
        )

    def tearDown(self):
        shutil.rmtree(os.path.dirname(self.dest_proj1))
        for d in os.listdir(self.staging_dir):
            fp = os.path.join(self.staging_dir, d)
            if os.path.isdir(fp):
                shutil.rmtree(fp)
            else:
                os.unlink(fp)

    def test_link_fastq_files(self):
        source = os.path.join(self.dest_proj1, sample1.get('sample_id'))
        self.gel_data_delivery_dry.link_fastq_files(
            source,
            self.staging_dir,
            sample1.get('user_sample_id'),
            sample1.get('user_sample_id')
        )
        assert os.path.islink(os.path.join(self.staging_dir, sample1.get('user_sample_id') + '_R1.fastq.gz'))
        assert os.path.islink(os.path.join(self.staging_dir, sample1.get('user_sample_id') + '_R2.fastq.gz'))

        # Mixing sample 1 and sample 2 id won't work
        source = os.path.join(self.dest_proj1, sample2.get('sample_id'))
        with pytest.raises(FileNotFoundError):
            self.gel_data_delivery_dry.link_fastq_files(
                source,
                self.staging_dir,
                sample1.get('user_sample_id'),
                sample1.get('user_sample_id')
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

    def test_get_delivery_id(self):
        with patch.object(GelDataDelivery, 'deliver_db', new_callable=PropertyMock()) as deliver_db:
            deliver_db.create_delivery = Mock(return_value=5)
            assert self.gel_data_delivery.get_delivery_id('s', 'e') == 'ED00000005'


    def test_deliver_data_dry(self):
        with patch('upload_to_gel.deliver_data_to_gel.GelDataDelivery.get_sample', new=fake_get_sample), \
             patch('upload_to_gel.deliver_data_to_gel.GelDataDelivery.info') as mock_info:
            self.gel_data_delivery_dry.deliver_data()
            mock_info.assert_any_call('Create delivery id from sample_id=sample1')
            mock_info.assert_any_call('Create delivery plateform sample_barcode=123456789_ext_sample1')
            mock_info.assert_called_with('Run rsync')


    def test_deliver_data(self):
        with patch('upload_to_gel.deliver_data_to_gel.GelDataDelivery.get_sample', new=fake_get_sample), \
             patch.object(GelDataDelivery, 'get_delivery_id', side_effect = ['ED01', 'ED01', 'ED02', 'ED02', 'ED03', 'ED03']), \
             patch('upload_to_gel.deliver_data_to_gel.send_action_to_rest_api'), \
             patch('egcg_core.executor.local_execute'):
            self.gel_data_delivery.deliver_data()
