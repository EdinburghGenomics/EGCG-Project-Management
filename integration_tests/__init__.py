import os
import sys
import json
import pytest
import requests
import argparse
import subprocess
from io import StringIO
from time import sleep
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock
from contextlib import redirect_stdout
from egcg_core import notifications, util, rest_communication
from egcg_core.config import Configuration

integration_cfg = Configuration(os.getenv('INTEGRATIONCONFIG'))


class NamedMock(Mock):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name


class IntegrationTest(TestCase):
    container_id = None

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

    def tearDown(self):
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

    start_time = now()
    s = StringIO()
    with redirect_stdout(s):
        exit_status = pytest.main([os.path.join(os.path.dirname(__file__), t) for t in tests])
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
