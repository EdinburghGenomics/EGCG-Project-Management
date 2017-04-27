import hashlib
import os
from unittest.mock import patch, PropertyMock, Mock

import shutil
from egcg_core.config import cfg
from egcg_core.constants import ELEMENT_SAMPLE_EXTERNAL_ID

from tests import TestProjectManagement
from upload_to_gel.deliver_data_to_gel import send_action_to_rest_api, GelDataDelivery

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

def mocked_get_sample(self, sample_id):
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


class TestGelDataDelivery(TestProjectManagement):

    def __init__(self, *args, **kwargs):
        super(TestProjectManagement, self).__init__(*args, **kwargs)
        cfg.load_config_file(os.path.join(os.path.dirname(self.root_test_path), 'etc', 'example_gel_data_delivery.yaml'))
        os.chdir(os.path.dirname(self.root_test_path))
        self.assets_delivery = os.path.join(self.assets_path, 'data_delivery')

    def setUp(self):
        batch_dir = os.path.join(self.assets_delivery, 'dest', 'project1', 'batch1')
        os.makedirs(batch_dir, exist_ok=True)
        for sample in ['sample1', 'sample2', 'sample3']:
            sample_dir = os.path.join(batch_dir, sample)
            os.makedirs(sample_dir, exist_ok=True)
            for suffix in ['_R1.fastq.gz', '_R2.fastq.gz', ]:
                f = os.path.join(sample_dir,samples.get(sample).get(ELEMENT_SAMPLE_EXTERNAL_ID) + suffix)
                touch(f)
                md5(f)
        self.gel_data_delivery_dry = GelDataDelivery(
            'project1',
            'batch1',
            dry_run=True,
            work_dir=os.path.join(self.assets_delivery, 'staging'),
            no_cleanup=True
        )
        self.gel_data_delivery = GelDataDelivery(
            'project1',
            'batch1',
            dry_run=False,
            work_dir=os.path.join(self.assets_delivery, 'staging'),
            no_cleanup=True
        )

    def tearDown(self):
        shutil.rmtree(os.path.join(self.assets_delivery, 'dest', 'project1'))
        staging = os.path.join(self.assets_delivery, 'staging')
        for d in os.listdir(staging):
            shutil.rmtree(os.path.join(staging, d))

    def test_link_fastq_files(self):
        pass

    @patch('upload_to_gel.deliver_data_to_gel.DeliveryAPIClient')
    def test_send_action_to_rest_api(self, mocked_request):
        send_action_to_rest_api('create', delivery_id='ED0000000001', sample_id='sample1')

    def test_get_delivery_id(self):
        with patch.object(GelDataDelivery, 'deliver_db', new_callable=PropertyMock()) as deliver_db:
            deliver_db.create_delivery = Mock(return_value=5)
            assert self.gel_data_delivery.get_delivery_id('s', 'e') == 'ED0000000005'


    def test_deliver_data_dry(self):
        with patch('upload_to_gel.deliver_data_to_gel.GelDataDelivery.get_sample', new=mocked_get_sample):
            self.gel_data_delivery_dry.deliver_data()

    def test_deliver_data(self):
        with patch('upload_to_gel.deliver_data_to_gel.GelDataDelivery.get_sample', new=mocked_get_sample), \
             patch.object(GelDataDelivery, 'get_delivery_id', side_effect = ['ED01', 'ED01', 'ED02', 'ED02', 'ED03', 'ED03']), \
             patch('upload_to_gel.deliver_data_to_gel.send_action_to_rest_api'), \
             patch('egcg_core.executor.local_execute'):
            self.gel_data_delivery.deliver_data()
