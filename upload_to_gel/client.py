"""TODO: module doc..."""
import os
import sys
import json
import requests
import argparse
import getpass
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, HTTPError
from egcg_core.app_logging import AppLogger


class ArgumentException(Exception):
    """The client was called with incorrect arguments."""
    pass


class DeliveryAPIClient(AppLogger):
    """TODO: class doc..."""

    valid_actions = ('all', 'get', 'awaiting_delivery', 'create', 'delivered', 'md5_passed', 'qc_passed',
                     'delete_permitted', 'deleted', 'upload_failed', 'md5_failed', 'qc_failed')

    def __init__(self, host, user, pswd, action, filter=None, delivery_id=None, sample_id=None, failure_reason=None):
        self.action = action
        self.filter = filter
        self.delivery_id = delivery_id
        self.sample_id = sample_id
        self.failure_reason = failure_reason
        self.valid_actions_string = ', '.join(self.valid_actions)
        self.auth = HTTPBasicAuth(user, pswd)
        self.base_url = 'http://' + host + '/api/deliveries'

        if self.action not in self.valid_actions:
            raise ArgumentException(
                'Action specified is not valid. You must use one of: ' +
                self.valid_actions_string
            )

        if not self.delivery_id and self.action != 'all':
            raise ArgumentException('You need to pass a delivery id')

        if not sample_id and self.action not in ('all', 'get', 'create'):
            raise ArgumentException('You need to pass a sample id')

    def make_call(self):
        """
        TODO method doc
        """
        if self.action == 'all':
            return self.get_all()

        elif self.action == 'get':
            return self.get(self.delivery_id)

        elif self.action == 'create':
            return self.create(self.delivery_id)

        elif self.action.endswith('failed'):
            if not self.failure_reason:
                raise ArgumentException(
                    'You need to pass a failure_reason')
            return self.do_failure(self.action, self.delivery_id, self.sample_id, self.failure_reason)

        else:
            return self.do_action(self.action, self.delivery_id, self.sample_id)

    def create(self, delivery_id):
        """
        TODO method doc
        :param delivery_id:
        """
        return self.do_http_call('put', self.get_url(delivery_id), json={'delivery_id': delivery_id})

    def get(self, delivery_id: object) -> object:
        """
        TODO method doc
        :param delivery_id:
        """
        r = self.do_http_call('get', self.get_url(delivery_id))
        self.debug(str(json.dumps(r.json(), indent=4)))
        return r

    def get_all(self):
        """
        TODO method doc
        """
        params = {'params': {'state': self.filter}} if self.filter else {}
        r = self.do_http_call('get', self.base_url, **params)
        self.debug(str(json.dumps(r.json(), indent=4)))
        return r

    def do_action(self, action, delivery_id, sample_id):
        """
        TODO method doc
        :param action:
        :param delivery_id:
        :param sample_id:
        """
        return self.do_http_call(
            'post',
            self.get_url(delivery_id, 'actions', action),
            json={
                'delivery_id': delivery_id,
                # needs to be sample_barcode for compatibility with code on the Illumina side
                'sample_barcode': sample_id
            }
        )

    def do_failure(self, action, delivery_id, sample_id, failure_reason):
        """
        TODO method doc
        :param action:
        :param delivery_id:
        :param sample_id:
        :param failure_reason:
        """
        return self.do_http_call(
            'post',
            self.get_url(delivery_id, 'actions', action),
            json={
                'delivery_id': delivery_id,
                # needs to be sample_barcode for compatibility with code on the Illumina side
                'sample_barcode': sample_id,
                'failure_reason': failure_reason
            }
        )

    def get_url(self, *parts):
        """
        TODO method doc
        :param parts:
        :return:
        """
        return '/'.join([self.base_url] + list(parts)) + '/'

    def do_http_call(self, http_method, url, **params):
        """
        TODO method doc
        :param http_method:
        :param url:
        :return:
        """
        method = getattr(requests, http_method)
        try:
            self.debug('Sending %s request to %s', http_method, url)
            r = method(url, auth=self.auth, **params)
        except ConnectionError as e:
            self.error('Connection error. Is the server running?')
            raise e
        self.debug('Status code: %s', r.status_code)
        try:
            r.raise_for_status()
        except HTTPError as e:
            self.error('Response text: {}'.format(r.text))
            raise e
        return r


def main():
    try:
        client = DeliveryAPIClient(**get_args())
        client.make_call()
    except KeyboardInterrupt:
        print('Stopped')


def get_args(argv=None):
    """
    TODO method doc
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='''
Example usage:
python client.py --action=all
python client.py --action=create --delivery_id=DELIVERY_1 --sample_id=SAMPLE_1
python client.py --action=delivered --delivery_id=DELIVERY_1 --sample_id=SAMPLE_1
python client.py --action=deleted --delivery_id=DELIVERY_1 --sample_id=SAMPLE_1
python client.py --action=upload_failed --delivery_id=DELIVERY_1 --sample_id=SAMPLE_1 --failure_reason='ran out of disk space'
    ''')

    parser.add_argument(
        '--host',
        help='host name and port to connect to, in the format host:port',
        default=os.getenv('GE_API_HOST')
    )
    parser.add_argument(
        '--user',
        help='which user to authenticate with',
        default=os.getenv('GE_API_USER')
    )
    parser.add_argument('--action', help='which method to call [{}]'.format(', '.join(DeliveryAPIClient.valid_actions)))
    parser.add_argument('--filter', help='status to filter by for --action=all call')
    parser.add_argument('--delivery_id', help='Delivery id for the target delivery')
    parser.add_argument('--sample_id', help='Sample id for the target delivery')
    parser.add_argument('--failure_reason', help='failure message containing the reason in the case of failure actions')
    args = parser.parse_args(argv)

    host = args.host
    user = args.user
    pswd = os.getenv('GE_API_PASS')

    action = args.action

    if not pswd:
        pswd = getpass.getpass('Password:')

    if not all([host, user, pswd, action]):
        print('Incorrect parameters provided: host, user, password and action are all required\n')
        parser.print_help()
        sys.exit()

    args.pswd = pswd
    return vars(args)


if __name__ == '__main__':
    main()
