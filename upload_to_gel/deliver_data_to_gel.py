import os
import re
import glob
import shutil
import sqlite3
from requests.exceptions import HTTPError
from cached_property import cached_property
from egcg_core import executor, clarity, rest_communication
from egcg_core.config import cfg
from egcg_core.app_logging import AppLogger
from egcg_core.constants import ELEMENT_SAMPLE_EXTERNAL_ID, ELEMENT_PROJECT_ID
from upload_to_gel.client import DeliveryAPIClient


SUCCESS_KW = 'passed'
FAILURE_KW = 'failed'


class DeliveryDB:
    schema = '''CREATE TABLE IF NOT EXISTS delivery(
       id INTEGER PRIMARY KEY,
       sample_id TEXT,
       external_sample_id TEXT,
       creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
       upload_state TEXT DEFAULT NULL,
       upload_confirm_date DATETIME DEFAULT NULL,
       md5_state TEXT DEFAULT NULL,
       md5_confirm_date DATETIME DEFAULT NULL,
       qc_state TEXT DEFAULT NULL,
       qc_confirm_date DATETIME DEFAULT NULL,
       failure_reason TEXT DEFAULT NULL
    );'''

    def __init__(self):
        self.delivery_db = sqlite3.connect(cfg.query('gel_upload', 'delivery_db'))
        self.cursor = self.delivery_db.cursor()
        self.cursor.execute(self.schema)

    @staticmethod
    def _delivery_number_to_id(delivery_number):
        return 'ED%08d' % delivery_number

    @staticmethod
    def _delivery_id_to_number(delivery_id):
        return int(delivery_id[2:])

    def create_delivery(self, sample_id, external_sample_id):
        q = 'INSERT INTO delivery (sample_id, external_sample_id) VALUES (?, ?);'
        self.cursor.execute(q, (sample_id, external_sample_id))
        self.delivery_db.commit()
        return self._delivery_number_to_id(self.cursor.lastrowid)

    def get_info_from(self, delivery_id, *fields):
        assert all(f in self.schema for f in fields)
        selection = ', '.join(fields) if fields else '*'
        query = 'SELECT ' + selection + ' FROM delivery WHERE id=?'
        self.cursor.execute(query, (self._delivery_id_to_number(delivery_id),))
        return self.cursor.fetchone()

    def get_most_recent_delivery_id(self, sample_id):
        q = 'SELECT id from delivery WHERE sample_id=? ORDER BY creation_date DESC LIMIT 1;'
        self.cursor.execute(q, (sample_id,))
        val = self.cursor.fetchone()
        if val:
            return self._delivery_number_to_id(val[0])
        return None

    def set_upload_state(self, delivery_id, state):
        q = 'UPDATE delivery SET upload_state = ?, upload_confirm_date = datetime("now") WHERE id = ?;'
        self.cursor.execute(q, (state, self._delivery_id_to_number(delivery_id)))
        self.delivery_db.commit()

    def set_md5_state(self, delivery_id, state):
        q = 'UPDATE delivery SET md5_state = ?, md5_confirm_date = datetime("now") WHERE id = ?;'
        self.cursor.execute(q, (state, self._delivery_id_to_number(delivery_id)))
        self.delivery_db.commit()

    def set_qc_state(self, delivery_id, state):
        q = 'UPDATE delivery SET qc_state = ?, qc_confirm_date = datetime("now") WHERE id = ?;'
        self.cursor.execute(q, (state, self._delivery_id_to_number(delivery_id)))
        self.delivery_db.commit()

    def report_all(self):
        self.cursor.execute('SELECT * FROM delivery')
        keys = ('id', 'sample_id', 'external_sample_id', 'creation_date', 'upload_state', 'upload_confirm_date',
                'md5_state', 'md5_confirm_date', 'qc_state', 'qc_confirm_date', 'failure_reason')

        print('\t'.join(keys))
        for delivery in self.cursor.fetchall():
            print('\t'.join(str(f) for f in delivery))

    def __del__(self):
        self.delivery_db.close()


class GelDataDelivery(AppLogger):
    def __init__(self, sample_id, user_sample_id=None, work_dir=None, dry_run=False, no_cleanup=False, force_new_delivery=False):
        self.sample_id = sample_id or self.resolve_sample_id(user_sample_id)
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
        did = self.deliver_db.get_most_recent_delivery_id(self.sample_id)
        if not did or self.force_new_delivery:
            did = self.deliver_db.create_delivery(self.sample_id, self.external_id)
        return did

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
        eid = self.sample_data[ELEMENT_SAMPLE_EXTERNAL_ID]
        m = re.match('[0-9]{9}_[0-9A-Za-z]{7}_[0-9]{4}_[0-9A-Za-z]{10}', eid)
        if not m:
            self.error('%s does not match the required regex', eid)
        return eid

    @property
    def external_id(self):
        return self.sample_data[ELEMENT_SAMPLE_EXTERNAL_ID]

    def cleanup(self):
        if self.no_cleanup:
            return
        if os.path.exists(self.staging_dir):
            shutil.rmtree(self.staging_dir)

    def link_fastq_files(self, fastq_path):
        for i in (1, 2):
            source = os.path.join(self.sample_delivery_folder, self.external_id + '_R%s.fastq.gz' % i)
            if not os.path.isfile(source):
                raise FileNotFoundError(source + ' does not exists')
            os.symlink(source, os.path.join(fastq_path, self.sample_barcode + '_R%s.fastq.gz' % i))

    def create_md5sum_txt(self, sample_path):
        with open(os.path.join(sample_path, 'md5sum.txt'), 'w') as fh:
            for i in (1, 2):
                with open(os.path.join(self.sample_delivery_folder, self.external_id + '_R%s.fastq.gz.md5' % i)) as f:
                    md5, fp = f.readline().strip().split()
                    fh.write('%s %s\n' % (md5, 'fastq/' + self.sample_barcode + '_R%s.fastq.gz' % i))

    @staticmethod
    def rsync_to_destination(delivery_id_path):
        options = ['-rv', '-L', '--timeout=300', '--append', '--partial', '--chmod ug+rwx,o-rwx', '--perms']
        ssh_options = ['ssh', '-o StrictHostKeyChecking=no', '-o TCPKeepAlive=yes', '-o ServerAliveInterval=100',
                       '-o KeepAlive=yes', '-o BatchMode=yes', '-o LogLevel=Error',
                       '-i %s' % cfg.query('gel_upload', 'ssh_key'), '-p 22']
        destination = '%s@%s:%s' % (cfg.query('gel_upload', 'username'), cfg.query('gel_upload', 'host'), cfg.query('gel_upload', 'dest'))

        cmd = ' '.join(['rsync',  ' '.join(options), '-e "%s"' % ' '.join(ssh_options), delivery_id_path, destination])
        return executor.local_execute(cmd).join()

    def try_rsync(self, delivery_id_path, max_nb_tries=3):
        tries = 1
        exit_code = 9
        while tries <= max_nb_tries:
            exit_code = self.rsync_to_destination(delivery_id_path)
            if exit_code == 0:
                return exit_code
            tries += 1
        return exit_code

    def delivery_id_exists(self):
        try:
            send_action_to_rest_api(action='get', delivery_id=self.delivery_id)
            return True
        except HTTPError:
            return False

    def deliver_data(self):
        # external sample_id is what GEL used as a sample barcode
        delivery_id_path = os.path.join(self.staging_dir, self.delivery_id)
        sample_path = os.path.join(delivery_id_path, self.sample_barcode)
        fastq_path = os.path.join(sample_path, 'fastq')

        os.makedirs(fastq_path, exist_ok=True)
        self.link_fastq_files(fastq_path)
        self.create_md5sum_txt(sample_path)

        if not self.dry_run:
            if not self.delivery_id_exists():
                send_action_to_rest_api(action='create', delivery_id=self.delivery_id, sample_id=self.sample_barcode)
            exit_code = self.try_rsync(delivery_id_path)
            self.info('Rsync exit code is %s', exit_code)
            if exit_code == 0:
                self.deliver_db.set_upload_state(self.delivery_id, SUCCESS_KW)
                send_action_to_rest_api(action='delivered', delivery_id=self.delivery_id, sample_id=self.sample_barcode)
            else:
                self.deliver_db.set_upload_state(self.delivery_id, FAILURE_KW)
                send_action_to_rest_api(
                    action='upload_failed',
                    delivery_id=self.delivery_id,
                    sample_id=self.sample_barcode,
                    failure_reason='rsync returned %s exit code' % exit_code
                )
        else:
            self.info('Create delivery id %s from sample_id=%s', self.delivery_id, self.sample_id)
            self.info('Create delivery platform sample_barcode=%s', self.sample_barcode)
            self.info('Run rsync')
        self.cleanup()

    def check_delivery_data(self):
        info = self.deliver_db.get_info_from(
            self.delivery_id,
            'upload_state', 'md5_state', 'md5_confirm_date', 'qc_state', 'qc_confirm_date'
        )
        upload_state, md5_state, md5_confirm_date, qc_state, qc_confirm_date = info
        if upload_state == SUCCESS_KW and not all((md5_confirm_date, qc_confirm_date)):
            sample = send_action_to_rest_api(action='get', delivery_id=self.delivery_id).json()
            param, status = sample['state'].split('_')  # md5_passed -> ('md5', 'passed')
            if param == 'md5':
                self.deliver_db.set_md5_state(self.delivery_id, status)
            elif param == 'qc':
                self.deliver_db.set_md5_state(self.delivery_id, SUCCESS_KW)  # if qc has passed, md5 must have passed
                self.deliver_db.set_qc_state(self.delivery_id, status)

            self.info('Delivery %s sample %s %s check: %s', self.delivery_id, self.sample_id, param, status)

        elif not upload_state:
            self.error('Delivery %s sample %s: Has not been uploaded', self.delivery_id, self.sample_id)
        elif upload_state == FAILURE_KW:
            self.error('Delivery %s sample %s: Upload failed', self.delivery_id, self.sample_id)
        elif qc_state:
            self.error('Delivery %s sample %s qc check failed - was checked before on %s', self.delivery_id, self.sample_id, qc_confirm_date)
        elif md5_state:
            self.error('Delivery %s sample %s md5 check failed - was checked before on %s', self.delivery_id, self.sample_id, md5_confirm_date)


def check_all_deliveries():
    delivery_db = DeliveryDB()
    delivery_db.cursor.execute('SELECT sample_id from delivery WHERE upload_state=? AND qc_state IS NULL', (SUCCESS_KW,))
    samples = delivery_db.cursor.fetchall()
    if samples:
        for sample_id, in samples:
            dd = GelDataDelivery(sample_id)
            dd.check_delivery_data()


def report_all():
    delivery_db = DeliveryDB()
    delivery_db.report_all()


def send_action_to_rest_api(action, **kwargs):
    api_param = cfg['gel_upload']['rest_api']
    # host, user, pswd from config
    api_param['action'] = action
    api_param.update(kwargs)
    client = DeliveryAPIClient(**api_param)
    return client.make_call()
