import os
import sys
import json
import pytest
import argparse
import subprocess
from time import sleep
from io import StringIO
from shutil import rmtree
from datetime import datetime
from contextlib import redirect_stdout, contextmanager
from egcg_core import rest_communication, notifications, util
from egcg_core.config import cfg, Configuration
from unittest import TestCase
from unittest.mock import Mock, patch
from bin import confirm_delivery


integration_cfg = Configuration(os.getenv('INTEGRATIONCONFIG'))
src_dir = os.path.dirname(__file__)
downloaded_files = os.path.join(src_dir, 'downloaded_files.csv')


def now():
    return datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')


def execute(*cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err:
        raise ValueError(err)
    return out.decode('utf-8').rstrip('\n')


class NamedMock(Mock):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name


@contextmanager
def patches():
    _patches = []

    def _patch(ppath, **kwargs):
        _p = patch(ppath, **kwargs)
        _p.start()
        _patches.append(_p)

    _patch('bin.confirm_delivery.load_config')
    _patch('bin.confirm_delivery.clarity.connection')
    _patch('bin.confirm_delivery.clarity.get_workflow_stage')
    _patch(
        'bin.confirm_delivery.Queue',
        return_value=Mock(
            artifacts=[
                Mock(samples=[NamedMock('sample_1')]),
                Mock(samples=[NamedMock('sample_2')])
            ]
        )
    )

    yield

    for p in _patches:
        p.stop()


class TestConfirmDelivery(TestCase):
    container_id = None
    delivered_projects = os.path.join(src_dir, 'delivered_projects')

    samples = [
        {
            'sample_id': 'sample_1',
            'project_id': 'project_1',
            'files_delivered': [
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_1/sample_1_R1.fastq.gz', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_1/sample_1_R2.fastq.gz', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_1/sample_1.bam', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_1/sample_1.g.vcf.gz', 'size': 1000000000}
            ]
        },
        {
            'sample_id': 'sample_2',
            'project_id': 'project_1',
            'files_delivered': [
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_2/sample_2_R1.fastq.gz', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_2/sample_2_R2.fastq.gz', 'size': 1000000000}
            ]
        },
        {
            'sample_id': 'sample_3',
            'project_id': 'project_1',
            'files_delivered': [
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_3/sample_3.bam', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_3/sample_3.g.vcf.gz', 'size': 1000000000}
            ]
        }
    ]

    @classmethod
    def setUpClass(cls):
        cfg.content = {
            'sample': {},
            'delivery_dest': cls.delivered_projects,  # TODO: really!?
            'delivery': {
                'dest': cls.delivered_projects
            }
        }
        os.makedirs(cls.delivered_projects, exist_ok=True)
        for s in cls.samples:
            for d in s['files_delivered']:
                f = os.path.join(cls.delivered_projects, d['file_path'])
                os.makedirs(os.path.dirname(f), exist_ok=True)
                open(f, 'w').close()

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

        sleep(30)  # allow time for the container's database and API to start running

        for s in self.samples:
            rest_communication.post_entry('samples', s)

    def tearDown(self):
        assert self.container_id
        execute('docker', 'stop', self.container_id)
        execute('docker', 'rm', self.container_id)

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.delivered_projects)

    def test_samples(self):
        with patches():
            confirm_delivery.main(
                ['--csv_files', downloaded_files, '--samples', 'sample_1', 'sample_2']
            )
        self._check_outputs()

    def test_all_queued_samples(self):
        with patches():
            confirm_delivery.main(
                ['--csv_files', downloaded_files, '--queued_samples']
            )
        self._check_outputs()

    @staticmethod
    def _check_outputs():
        for sample_id in ('sample_1', 'sample_2'):
            obs = rest_communication.get_document('samples', where={'sample_id': sample_id})
            for ext in ('_R1.fastq.gz', '_R2.fastq.gz', '.bam', '.g.vcf.gz'):
                exp = {
                    'date': '05_12_2017_09:05:00',
                    'user': 'a_remote_aspera_user',
                    'file_path': 'project_1/delivery_date/{s}/{s}{ext}'.format(s=sample_id, ext=ext),
                    'size': 1000000000
                }
                assert exp in obs['files_downloaded']

        obs = rest_communication.get_document('samples', where={'sample_id': 'sample_3'})
        for ext in ('.bam', '.g.vcf.gz'):
            exp = {
                'date': '05_12_2017_09:05:00',
                'user': 'a_remote_aspera_user',
                'file_path': 'project_1/delivery_date/sample_3/sample_3{ext}'.format(ext=ext),
                'size': 1000000000
            }
            assert exp in obs['files_downloaded']


def main():
    a = argparse.ArgumentParser()
    a.add_argument('--stdout', action='store_true')
    a.add_argument('--email', action='store_true')
    a.add_argument('--log_repo')
    args = a.parse_args()

    start_time = now()
    s = StringIO()
    with redirect_stdout(s):
        exit_status = pytest.main([__file__])
    end_time = now()

    test_output = util.str_join(
        'Pipeline end-to-end test finished',
        'Run on commit %s' % execute('git', 'log', "--format=%h on%d, made on %aD", '-1'),
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
        notifications.send_email(
            test_output, subject='Analysis Driver integration test', **integration_cfg['notification']
        )

    return exit_status


if __name__ == '__main__':
    sys.exit(main())
