import os
import sys
import json
import pytest
import requests
import argparse
import subprocess
from io import StringIO
from time import sleep
from shutil import rmtree
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock
from collections import defaultdict
from contextlib import redirect_stdout
from egcg_core import notifications, util, rest_communication, archive_management
from egcg_core.config import Configuration

integration_cfg = Configuration(os.getenv('INTEGRATIONCONFIG'))


class NamedMock(Mock):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name


class IntegrationTest(TestCase):
    container_id = None
    patches = ()

    def setUp(self):
        assert self.container_id is None
        self.container_id = execute(
            'docker', 'run', '-d', integration_cfg['reporting_app']['image_name'],
            integration_cfg.query('reporting_app', 'branch', ret_default='master')
        )
        assert self.container_id
        container_info = json.loads(execute('docker', 'inspect', self.container_id))[0]
        container_ip = container_info['NetworkSettings']['Networks']['bridge']['IPAddress']
        container_port = list(container_info['Config']['ExposedPorts'])[0].rstrip('/tcp')
        container_url = 'http://' + container_ip + ':' + container_port + '/api/0.1'
        rest_communication.default._baseurl = container_url
        rest_communication.default._auth = ('apiuser', 'apiuser')

        self._ping(container_url)

        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()

        assert self.container_id
        execute('docker', 'stop', self.container_id)
        execute('docker', 'rm', self.container_id)

    def _ping(self, url, retries=36):
        try:
            requests.get(url, timeout=2)
            return True
        except requests.exceptions.ConnectionError:
            if retries > 0:
                sleep(5)
                return self._ping(url, retries - 1)
            else:
                raise


def setup_delivered_samples(processed_dir, delivery_dir, fastq_dir):
    for d in (processed_dir, delivery_dir, fastq_dir):
        if os.path.isdir(d):
            rmtree(d)

    all_files = defaultdict(list)

    def _setup_delivered_sample(i, fluidx_barcode=None):
        sample_id = 'sample_' + str(i)
        ext_sample_id = 'ext_' + sample_id
        sample_dir = os.path.join(processed_dir, 'a_project', sample_id)
        delivered_dir = os.path.join(delivery_dir, 'a_project', 'a_delivery_date', fluidx_barcode or sample_id)
        sample_fastq_dir = os.path.join(fastq_dir, 'a_run', 'a_project', sample_id)

        os.makedirs(sample_dir)
        os.makedirs(sample_fastq_dir)
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
            all_files[sample_id].append(f)

        for r in ('1', '2'):
            f = os.path.join(sample_fastq_dir, 'L00%s_R%s.fastq.gz' % (i, r))
            all_files[sample_id].append(f)

        for f in all_files[sample_id]:
            open(f, 'w').close()
            os.link(f, os.path.join(delivered_dir, os.path.basename(f)))
            archive_management.register_for_archiving(f)

    _setup_delivered_sample(1)
    _setup_delivered_sample(2)
    _setup_delivered_sample(3)
    _setup_delivered_sample(4, 'sample_4_2d_barcode')

    for sample_id in ('sample_1', 'sample_2', 'sample_3', 'sample_4'):
        for f in all_files[sample_id]:
            while not archive_management.is_archived(f):
                sleep(10)

    return all_files


def setup_samples_deleted_from_tier1(processed_dir, delivered_dir, fastq_dir, processed_archive_dir, fastq_archive_dir):
    all_files = setup_delivered_samples(processed_dir, delivered_dir, fastq_dir)
    for d in (processed_archive_dir, fastq_archive_dir):
        if os.path.isdir(d):
            rmtree(d)
        os.makedirs(d)

    # Remove the delivered data
    rmtree(os.path.join(delivered_dir, 'a_project', 'a_delivery_date'))
    samples = ('sample_1', 'sample_2', 'sample_3', 'sample_4')

    for sample_id in samples:
        # remove files from lustre
        for f in all_files[sample_id]:
            archive_management.release_file_from_lustre(f)

        # mark the sample as deleted on rest api
        rest_communication.patch_entry('samples', {'data_deleted': 'on lustre'}, 'sample_id', sample_id)

    for sample_id in samples:
        for f in all_files[sample_id]:
            while not archive_management.is_released(f):
                sleep(10)

    return all_files


def now():
    return datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')


def execute(*cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err:
        raise ValueError(err)
    return out.decode('utf-8').rstrip('\n')


def main():
    a = argparse.ArgumentParser()
    a.add_argument('--stdout', action='store_true')
    a.add_argument('--email', action='store_true')
    a.add_argument('--log_repo')
    a.add_argument('--test', nargs='+', default=[])
    a.add_argument('--ls', action='store_true')
    args = a.parse_args()

    test_files = util.find_files(os.path.dirname(__file__), '*', '*.py')
    tests = sorted(set(os.path.basename(os.path.dirname(t)) for t in test_files))

    if args.ls:
        print('Available tests: %s' % tests)
        return 0

    if args.test:
        tests = [t for t in tests if t in args.test]

    top_level = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(top_level)

    start_time = now()
    s = StringIO()
    with redirect_stdout(s):
        exit_status = pytest.main([os.path.join(os.path.dirname(__file__), t) for t in tests])
    end_time = now()

    test_output = util.str_join(
        'Pipeline end-to-end test finished',
        'Run on commit %s' % execute(
            'git',
            '--git-dir=%s' % os.path.join(top_level, '.git'),
            'log',
            "--format=%h on%d, made on %aD",
            '-1'
        ),
        'Start time: %s, finish time: %s' % (start_time, end_time),
        'Pytest output:',
        s.getvalue(),
        separator='\n'
    )

    if args.log_repo:
        with open(os.path.join(args.log_repo, start_time + '.log'), 'w') as f:
            f.write(test_output)

    if args.stdout:
        print(test_output)

    if args.email:
        notifications.send_plain_text_email(
            test_output, subject='EGCG-Project-Management integration test', **integration_cfg['notification']
        )

    return exit_status


if __name__ == '__main__':
    sys.exit(main())
