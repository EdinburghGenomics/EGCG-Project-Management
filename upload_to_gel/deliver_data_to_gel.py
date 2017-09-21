import glob
import os
import shutil

import sqlite3

from cached_property import cached_property
from egcg_core import executor, clarity
from egcg_core import rest_communication
from egcg_core.app_logging import AppLogger
from egcg_core.config import cfg
from egcg_core.constants import ELEMENT_SAMPLE_EXTERNAL_ID, ELEMENT_PROJECT_ID
from upload_to_gel.client import DeliveryAPIClient


SUCCESS_KW = 'passed'
FAILURE_KW = 'failed'


class DeliveryDB:

    def __init__(self):
        self.delivery_db = sqlite3.connect(cfg.query('gel_upload', 'delivery_db'))
        self.cursor = self.delivery_db.cursor()
        self.cursor.execute('''CREATE TABLE  IF NOT EXISTS delivery(
           id INTEGER PRIMARY KEY,
           sample_id TEXT,
           external_sample_id TEXT,
           creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
           upload_state TEXT DEFAULT NULL,
           upload_confirm_date DATETIME DEFAULT NULL,
           md5_state TEXT DEFAULT NULL,
           md5_confirm_date DATETIME DEFAULT NULL
        );''')

    def create_delivery(self, sample_id, external_sample_id):
        q = 'INSERT INTO delivery (sample_id, external_sample_id) VALUES (?, ?);'
        self.cursor.execute(q, (sample_id, external_sample_id))
        self.delivery_db.commit()
        return self.cursor.lastrowid

    def get_info_from(self, delivery_number):
        self.cursor.execute('SELECT * from delivery WHERE id=?', (delivery_number,))
        return self.cursor.fetchone()

    def get_sample_from(self, delivery_number):
        return self.get_info_from(delivery_number)[1]

    def get_creation_date_from(self, delivery_number):
        return self.get_info_from(delivery_number)[3]

    def get_upload_confirmation_from(self, delivery_number):
        return self.get_info_from(delivery_number)[4:6]

    def get_md5_confirmation_from(self, delivery_number):
        return self.get_info_from(delivery_number)[6:8]

    def get_most_recent_delivery_id(self, sample_id):
        q = 'SELECT id from delivery WHERE sample_id=? ORDER BY creation_date DESC LIMIT 1;'
        self.cursor.execute(q, (sample_id,))
        val = self.cursor.fetchone()
        if val:
            return val[0]
        return None

    def set_upload_state(self, delivery_number, state):
        q = 'UPDATE delivery SET upload_state = ?, upload_confirm_date = datetime("now") WHERE id = ?;'
        self.cursor.execute(q, (state, delivery_number))
        self.delivery_db.commit()

    def set_md5_state(self, delivery_number, state):
        q = 'UPDATE delivery SET md5_state = ?, md5_confirm_date = datetime("now") WHERE id = ?;'
        self.cursor.execute(q, (state, delivery_number))
        self.delivery_db.commit()

class GelDataDelivery(AppLogger):
    def __init__(self, work_dir, sample_id, user_sample_id=None, dry_run=False, no_cleanup=False, force_new_delivery=False):
        self.sample_id = sample_id
        if not self.sample_id:
            self.sample_id = self.resolve_sample_id(user_sample_id)
        self.dry_run = dry_run
        self.work_dir = work_dir
        self.no_cleanup = no_cleanup
        self.force_new_delivery = force_new_delivery

    @staticmethod
    def resolve_sample_id(user_sample_id):
        samples = clarity.connection().get_samples(udf={'User Sample Name': user_sample_id})
        if len(samples) != 1:
            raise ValueError('User sample name %s resolve to %s sample' % (user_sample_id, len(samples)))
        return samples[0].name

    @cached_property
    def project_id(self):
        return self.sample_data[ELEMENT_PROJECT_ID]

    @cached_property
    def staging_dir(self):
        return os.path.join(self.work_dir, 'data_delivery_' + self.project_id + '_' + self.sample_id)

    @cached_property
    def sample_data(self):
        return rest_communication.get_document(
            'samples',
            quiet=True,
            where={'sample_id': self.sample_id}
        )

    @cached_property
    def deliver_db(self):
        return DeliveryDB()

    @cached_property
    def delivery_id(self):
        if self.dry_run:
            return 'ED00TEST'
        delivery_number = self.deliver_db.get_most_recent_delivery_id(self.sample_id)
        if not delivery_number or self.force_new_delivery:
            delivery_number = self.deliver_db.create_delivery(self.sample_id, self.external_id)
        return 'ED%08d' % delivery_number

    @cached_property
    def fluidx_barcode(self):
        lims_sample = clarity.get_sample(self.sample_id)
        return lims_sample.udf.get('2D Barcode')


    @cached_property
    def sample_delivery_folder(self):
        delivery_dest = cfg.query('delivery', 'dest')
        if self.fluidx_barcode:
            path_to_glob = os.path.join(delivery_dest, self.project_id, '*', self.fluidx_barcode)
        else:
            path_to_glob = os.path.join(delivery_dest, self.project_id, '*', self.sample_id)
        tmp = glob.glob(path_to_glob)
        if len(tmp) == 1:
            return tmp[0]
        else:
            raise ValueError('Could not find a single delivery folder: %s ' % path_to_glob)

    @property
    def sample_barcode(self):
        return self.sample_data[ELEMENT_SAMPLE_EXTERNAL_ID]

    @property
    def external_id(self):
        return self.sample_data[ELEMENT_SAMPLE_EXTERNAL_ID]

    def cleanup(self):
        if self.no_cleanup:
            return
        if os.path.exists(self.staging_dir):
            shutil.rmtree(self.staging_dir)

    def link_fastq_files(self, fastq_path):
        source1 = os.path.join(self.sample_delivery_folder, self.external_id + '_R1.fastq.gz')
        source2 = os.path.join(self.sample_delivery_folder, self.external_id + '_R2.fastq.gz')
        if not os.path.isfile(source1):
            raise FileNotFoundError(source1 + ' does not exists')
        if not os.path.isfile(source2):
            raise FileNotFoundError(source2 + ' does not exists')
        os.symlink(source1, os.path.join(fastq_path, self.sample_barcode + '_R1.fastq.gz'))
        os.symlink(source2, os.path.join(fastq_path, self.sample_barcode + '_R2.fastq.gz'))

    def create_md5sum_txt(self, sample_path):
        with open(os.path.join(self.sample_delivery_folder, self.external_id + '_R1.fastq.gz.md5')) as fh:
            md5_1, fp = fh.readline().strip().split()
        with open(os.path.join(self.sample_delivery_folder, self.external_id + '_R2.fastq.gz.md5')) as fh:
            md5_2, fp = fh.readline().strip().split()
        with open(os.path.join(sample_path,'md5sum.txt'), 'w') as fh:
            fh.write('%s %s' % (md5_1, 'fastq/' + self.sample_barcode + '_R1.fastq.gz'))
            fh.write('%s %s' % (md5_2, 'fastq/' + self.sample_barcode + '_R2.fastq.gz'))

    def rsync_to_destination(self, delivery_id_path):
        options = ['-rv', '-L', '--timeout=300', '--append', '--partial', '--chmod ug+rwx,o-rwx', '--perms']
        ssh_options = ['ssh', '-o StrictHostKeyChecking=no', '-o TCPKeepAlive=yes', '-o ServerAliveInterval=100',
                       '-o KeepAlive=yes', '-o BatchMode=yes', '-o LogLevel=Error',
                       '-i %s'%cfg.query('gel_upload', 'ssh_key'), '-p 22']
        destination = '%s@%s:%s' % (cfg.query('gel_upload', 'username'), cfg.query('gel_upload', 'host'), cfg.query('gel_upload', 'dest'))

        cmd = ' '.join(['rsync',  ' '.join(options), '-e "%s"' % ' '.join(ssh_options), delivery_id_path, destination])
        return executor.local_execute(cmd).join()

    def try_rsync(self, delivery_id_path, max_nb_tries=3):
        tries = 1
        while tries < max_nb_tries:
            exit_code = self.rsync_to_destination(delivery_id_path)
            if exit_code == 0:
                return exit_code
            tries += 1
        return exit_code

    def deliver_data(self):
        # external sample_id is what GEL used as a sample barcode
        delivery_id_path = os.path.join(self.staging_dir, self.delivery_id)
        sample_path = os.path.join(delivery_id_path, self.sample_barcode)
        fastq_path = os.path.join(sample_path, 'fastq')

        os.makedirs(fastq_path, exist_ok=True)
        self.link_fastq_files(fastq_path)
        self.create_md5sum_txt(sample_path)

        if not self.dry_run:
            send_action_to_rest_api(action='create', delivery_id=self.delivery_id, sample_id=self.sample_barcode)
            exit_code = self.try_rsync(delivery_id_path)
            if exit_code == 0:
                self.deliver_db.set_upload_state(self.delivery_id, SUCCESS_KW)
                send_action_to_rest_api(action='delivered', delivery_id=self.delivery_id, sample_id=self.sample_barcode)
            else:
                self.deliver_db.set_upload_state(self.delivery_id, FAILURE_KW)
                send_action_to_rest_api(
                    action='upload_failed',
                    delivery_id=self.delivery_id,
                    sample_id=self.sample_barcode,
                    failure_reason='rsync returned %s exit code' % (exit_code)
                )
        else:
            self.info('Create delivery id %s from sample_id=%s' % (self.delivery_id, self.sample_id,))
            self.info('Create delivery plateform sample_barcode=%s' % (self.sample_barcode,))
            self.info('Run rsync')
        self.cleanup()

    def check_md5sum(self):
        info = self.deliver_db.get_info_from(self.delivery_id)
        id, sample_id, external_sample_id, creation_date, upload_state, upload_confirm_date, md5_state, md5_confirm_date = info
        if upload_state == SUCCESS_KW and not md5_confirm_date:
            req = send_action_to_rest_api(action='get', delivery_id= self.delivery_id)
            sample_json = req.json()
            if sample_json['state'] == "md5_passed":
                self.deliver_db.set_md5_state(self.delivery_id, SUCCESS_KW)
                self.info('Delivery %s sample %s: md5 check has been successful', self.delivery_id, self.sample_id)
            elif sample_json['state'] == "md5_failed":
                self.deliver_db.set_md5_state(self.delivery_id, FAILURE_KW)
                self.info('Delivery %s sample %s: md5 check has failed', self.delivery_id, self.sample_id)
        elif not upload_state:
            self.error('Delivery %s sample %s: Has not been uploaded', self.delivery_id, self.sample_id)
        elif upload_state == FAILURE_KW:
            self.error('Delivery %s sample %s: Uploaded has failed', self.delivery_id, self.sample_id)
        elif md5_state:
            self.error('Delivery %s sample %s md5 check failed was checked before on ', self.delivery_id, self.sample_id, md5_confirm_date)


def check_all_md5sums(work_dir):
    delivery_db = DeliveryDB()
    delivery_db.crusor.execute('SELECT sample_id from delivery WHERE upload_state=? AND md5_state IS NULL', (SUCCESS_KW))
    samples = delivery_db.crusor.fetchall()
    if samples:
        for sample_id, in samples:
            dd = GelDataDelivery(work_dir, sample_id)
            dd.check_md5sum()

def send_action_to_rest_api(action, **kwargs):
    api_param = cfg.query('gel_upload', 'rest_api')
    # host, user, pswd from config
    api_param['action'] = action
    api_param.update(kwargs)
    client = DeliveryAPIClient(**api_param)
    return client.make_call()
