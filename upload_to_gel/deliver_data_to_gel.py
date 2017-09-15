import glob
import os
import shutil

import sqlite3

from cached_property import cached_property
from egcg_core import executor
from egcg_core import rest_communication
from egcg_core.app_logging import AppLogger
from egcg_core.config import cfg
from egcg_core.constants import  ELEMENT_SAMPLE_EXTERNAL_ID
from upload_to_gel.client import DeliveryAPIClient


class DeliveryDB:

    def __init__(self):
        self.delivery_db = sqlite3.connect(cfg.query('gel_upload', 'delivery_db'))
        self.cursor = self.delivery_db.cursor()
        self.cursor.execute('''CREATE TABLE delivery(
           id INTEGER AUTOINCREMENT,
           sample_id TEXT,
           external_sample_id TEXT,
           creation_date DATETIME DEFAULT CURRENT_TIMESTAMP
        );''')


    def create_delivery(self, sample_id, external_sample_id):
        self.cursor.execute('INSERT INTO delivery VALUES (?, ?)', (sample_id, external_sample_id))
        self.delivery_db.commit()
        return self.cursor.lastrowid


class GelDataDelivery(AppLogger):
    def __init__(self, project_id, sample_id, dry_run, work_dir, no_cleanup=False):
        self.project_id = project_id
        self.sample_id = sample_id
        self.dry_run = dry_run
        self.work_dir = work_dir
        self.staging_dir = os.path.join(self.work_dir, 'data_delivery_' + self.project_id + '_' + self.sample_id)
        self.no_cleanup = no_cleanup
        self._samples = {}
        self.samples_to_delivery_id = {}

    def cleanup(self):
        if self.no_cleanup:
            return
        if os.path.exists(self.staging_dir):
            shutil.rmtree(self.staging_dir)

    def link_fastq_files(self, original_delivery, fastq_path, external_id, sample_barcode):
        os.symlink(
            os.path.join(original_delivery, external_id + '_R1.fastq.gz'),
            os.path.join(fastq_path, sample_barcode +'_R1.fastq.gz')
        )
        os.symlink(
            os.path.join(original_delivery, external_id + '_R2.fastq.gz'),
            os.path.join(fastq_path, sample_barcode + '_R2.fastq.gz')
        )

    def create_md5sum_txt(self, original_delivery, sample_path, external_id, gel_id):
        with open(os.path.join(original_delivery, external_id + '_R1.fastq.gz.md5')) as fh:
            md5_1, fp = fh.readline().strip().split()
        with open(os.path.join(original_delivery, external_id + '_R2.fastq.gz.md5')) as fh:
            md5_2, fp = fh.readline().strip().split()
        with open(os.path.join(sample_path,'md5sums.txt'), 'w') as fh:
            fh.write('%s %s' % (md5_1, 'fastq/' + gel_id + '_R1.fastq.gz'))
            fh.write('%s %s' % (md5_2, 'fastq/' + gel_id + '_R2.fastq.gz'))

    def get_sample(self, sample_id):
        if not sample_id in self._samples:
            self._samples[sample_id] = rest_communication.get_document(
                'samples',
                quiet=True,
                where={'sample_id': sample_id}
            )
        return self._samples[sample_id]

    def rsync_to_destination(self, sample_path, delivery_id):
        options = ['-rv', '-L', '--timeout=300', '--append', '--partial', '--chmod ug+rwx,o-rwx', '--perms']
        ssh_options = ['-o StrictHostKeyChecking=no', '-o TCPKeepAlive=yes', '-o ServerAliveInterval=100',
                       '-o KeepAlive=yes', '-o BatchMode=yes', '-o LogLevel=Error',
                       '-i %s'%cfg.query('gel_upload', 'ssh_key'), '-p 22']
        destination = '%s@%s:upload/%s'%(cfg.query('gel_upload', 'username'), cfg.query('gel_upload', 'host'), delivery_id)

        cmd = ' '.join(['rsync',  ' '.join(options), '-e ssh "%s"' % ' '.join(ssh_options), sample_path, destination])
        return executor.local_execute(cmd).join()

    def try_rsync(self, sample_path, delivery_id, max_nb_tries=3):
        tries = 1
        while tries < max_nb_tries:
            exit_code = self.rsync_to_destination(sample_path, delivery_id)
            if exit_code == 0:
                return exit_code
            tries += 1
        return exit_code

    @cached_property
    def deliver_db(self):
        return DeliveryDB()

    def get_delivery_id(self, sample_id, external_sample_id):
        if sample_id not in self.samples_to_delivery_id:
            delivery_number = self.deliver_db.create_delivery(sample_id, external_sample_id)
            self.samples_to_delivery_id[sample_id] = 'ED%08d' % delivery_number
        return self.samples_to_delivery_id[sample_id]

    def get_sample_barcode(self, sample):
        return sample[ELEMENT_SAMPLE_EXTERNAL_ID]

    def deliver_data(self):
        delivery_dest = cfg.query('sample', 'delivery_dest')
        # find the batch directory
        sample_delivery_folder = os.path.join(delivery_dest, self.project_id, '*', self.sample_id)
        tmp = glob.glob(sample_delivery_folder)
        if len(tmp) == 1 :
            sample_delivery_folder = tmp[0]
        else:
            raise ValueError('Could not find delivery folder: %s ' % sample_delivery_folder)

        sample = self.get_sample(self.sample_id)
        external_id = sample[ELEMENT_SAMPLE_EXTERNAL_ID]
        # external sample_id is what GEL used as a sample barcode
        sample_barcode = self.get_sample_barcode(sample)
        sample_path = os.path.join(self.staging_dir, self.sample_id, sample_barcode)
        fastq_path = os.path.join(sample_path, 'fastq')

        os.makedirs(fastq_path, exist_ok=True)
        self.link_fastq_files(sample_delivery_folder, fastq_path, external_id, sample_barcode)
        self.create_md5sum_txt(sample_delivery_folder, sample_path, external_id, sample_barcode)

        if not self.dry_run:
            delivery_id = self.get_delivery_id(self.sample_id, external_sample_id=sample[ELEMENT_SAMPLE_EXTERNAL_ID])
            send_action_to_rest_api(action='create', delivery_id=delivery_id, sample_id=sample_barcode)
            exit_code = self.try_rsync(sample_path, delivery_id)
            if exit_code == 0:
                send_action_to_rest_api(action='delivered', delivery_id=delivery_id, sample_id=sample_barcode)
            else:
                send_action_to_rest_api(
                    action='upload_failed',
                    delivery_id=delivery_id,
                    sample_id=sample_barcode,
                    failurereason='rsync returned %s exit code' % (exit_code)
                )
        else:
            self.info('Create delivery id from sample_id=%s' % (self.sample_id,))
            self.info('Create delivery plateform sample_barcode=%s' % (sample_barcode,))
            self.info('Run rsync')

        if self.dry_run:
            return

        sample = self.get_sample(self.sample_id)
        if not sample.get('md5sum check'):
            delivery_id = self.get_delivery_id(self.sample_id)
            req = send_action_to_rest_api(action='get', delivery_id=delivery_id)
            sample_json = req.json()
            if sample_json['state'] == "md5_passed":
                sample['md5sum check'] == 'passed'
            elif sample_json['state'] == "md5_failed":
                sample['md5sum check'] == 'failed'

        self.cleanup()


def send_action_to_rest_api(action, **kwargs):
    api_param = cfg.query('gel_upload', 'rest_api')
    # host, user, pswd from config
    api_param['action'] = action
    api_param.update(kwargs)
    client = DeliveryAPIClient(**api_param)
    return client.make_call()
