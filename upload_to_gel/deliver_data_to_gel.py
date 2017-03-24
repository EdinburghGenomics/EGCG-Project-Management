import argparse
import datetime
import glob
import logging
import os
import shutil
import sys
from collections import defaultdict

from egcg_core import executor
from egcg_core import rest_communication, clarity
from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core.config import cfg
from egcg_core.constants import ELEMENT_NB_READS_CLEANED, ELEMENT_RUN_NAME, ELEMENT_PROJECT_ID, ELEMENT_LANE, \
    ELEMENT_SAMPLE_INTERNAL_ID, ELEMENT_SAMPLE_EXTERNAL_ID, ELEMENT_RUN_ELEMENT_ID, ELEMENT_USEABLE
from egcg_core.exceptions import EGCGError
from egcg_core.util import find_fastqs

from config import load_config
from upload_to_gel.client import DeliveryAPIClient

hs_list_files = [
    '{ext_sample_id}.g.vcf.gz',
    '{ext_sample_id}.g.vcf.gz.tbi',
    '{ext_sample_id}.vcf.gz',
    '{ext_sample_id}.vcf.gz.tbi',
    '{ext_sample_id}.bam',
    '{ext_sample_id}.bam.bai'
]

variant_call_list_files = [
    '{ext_sample_id}.g.vcf.gz',
    '{ext_sample_id}.g.vcf.gz.tbi',
    '{ext_sample_id}.bam',
    '{ext_sample_id}.bam.bai'
]

other_list_files = []


def _execute(*commands, **kwargs):
    exit_status = executor.execute(*commands, **kwargs).join()
    if exit_status != 0:
        raise EGCGError('commands %s exited with status %s' % (commands, exit_status))


class GelDataDelivery(AppLogger):
    def __init__(self, project_id, batch_id, dry_run, work_dir, no_cleanup=False):
        self.project_id = project_id
        self.batch_id = batch_id
        self.all_commands_for_cluster = []
        self.dry_run = dry_run
        self.work_dir = work_dir
        today = datetime.date.today().isoformat()
        self.staging_dir = os.path.join(self.work_dir, 'data_delivery_' + today)
        self.no_cleanup = no_cleanup

    def cleanup(self):
        if self.no_cleanup:
            return
        if os.path.exists(self.staging_dir):
            shutil.rmtree(self.staging_dir)

    def link_fastq_files(self, original_delivery, fastq_path, external_id, gel_id):
        os.link(
            os.path.join(original_delivery, external_id + '_R1.fastq.gz'),
            os.path.join(fastq_path, gel_id, '_R1.fastq.gz')
        )
        os.link(
            os.path.join(original_delivery, external_id + '_R2.fastq.gz'),
            os.path.join(fastq_path, gel_id, '_R1.fastq.gz')
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
            self._samples[sample_id] = rest_communication.get_documents(
                'samples',
                all_pages=True,
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

        cmd = ' '.join('rsync',  ' '.join(options), '-e ssh', ' '.join(ssh_options), sample_path, destination)
        return executor.local_execute(cmd).join()

    def deliver_data(self):
        delivery_dest = cfg.query('delivery_dest')
        # find the batch directory
        batch_delivery_folder = os.path.join(delivery_dest, self.project_id, self.batch_id)
        samples = os.listdir(batch_delivery_folder)
        # Retrieve the information about the sample from our database


        for sample_id in samples:
            sample = self.get_sample(sample_id)
            # external sample_id should be start with Gel id
            gel_id = sample[ELEMENT_SAMPLE_EXTERNAL_ID].split('_')[0]
            sample_path = os.path.join(self.staging_dir, self.batch_id, gel_id)
            fastq_path = os.path.join(sample_path, 'fastq')
            original_delivery = os.path.join(batch_delivery_folder, sample_id)
            os.makedirs(fastq_path, exist_ok=True)
            self.link_fastq_files(original_delivery, fastq_path)
            self.create_md5sum_txt(original_delivery, sample_path)

            delivery_id = gel_id + '_' + self.batch_id
            send_action_to_rest_api(action='create', delivery_id=delivery_id)
            exit_code = self.rsync_to_destination(sample_path, delivery_id)
            if exit_code == 0:
                send_action_to_rest_api(action='create', delivery_id=delivery_id)

def send_action_to_rest_api(action, **kwargs):
    api_param = cfg.query('gel_upload', 'rest_api')
    # host, user, pswd from config
    api_param['action'] = action
    api_param.update(kwargs)
    client = DeliveryAPIClient(**api_param)
    return client.make_call()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dry_run', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--work_dir', type=str, required=True)
    p.add_argument('--project_id', type=str)
    p.add_argument('--sample_id', type=str)
    args = p.parse_args()

    load_config()

    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)
        log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout))

    cfg.merge(cfg['sample'])

    GelDataDelivery()



if __name__ == '__main__':
    main()
