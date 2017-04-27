"""TODO: module doc..."""

from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, HTTPError
import requests
import argparse
import getpass
import json
import os
import sys


class ArgumentException(Exception):
    """The client was called with incorrect arguments."""
    pass


class DeliveryAPIClient:
    """TODO: class doc..."""

    valid_actions = [
        'all',
        'get',
        'awaiting_delivery',
        'create',
        'delivered',
        'md5_passed',
        'qc_passed',
        'delete_permitted',
        'deleted',
        'upload_failed',
        'md5_failed',
        'qc_failed']

    def __init__(self, host, user, pswd, action, filter=None, delivery_id=None, sample_id=None, failure_reason=None):
        self.host = host
        self.user = user
        self.pswd = pswd
        self.action = action
        self.filter = filter
        self.delivery_id = delivery_id
        self.sample_id = sample_id
        self.failure_reason = failure_reason
        self.valid_actions_string = ', '.join(self.valid_actions)
        self.init_http_params()


    def init_http_params(self):
        """
        TODO method doc
        """
        self.auth = HTTPBasicAuth(self.user, self.pswd)
        self.base_url = 'http://' + self.host + '/api/deliveries'
        self.headers = {'content-type': 'application/json'}
        self.params = {'auth': self.auth, 'headers': self.headers}

    def make_call(self):
        """
        TODO method doc
        """
        try:
            if self.action not in self.valid_actions:
                raise ArgumentException(
                    'Action specified is not a valid action. You must use one of: ' +
                    self.valid_actions_string)

            if self.action == 'all':
                return self.get_all(self.filter)
            elif self.action == 'get':
                if not self.delivery_id:
                    raise ArgumentException('You need to pass a delivery id')
                return self.get(self.delivery_id)
            elif self.action == 'create':
                if not self.delivery_id:
                    raise ArgumentException('You need to pass a delivery id')
                if not self.sample_id:
                    raise ArgumentException('You need to pass a sample id')
                return self.create(self.delivery_id, self.sample_id)

            elif self.action.endswith('failed'):
                if not (self.delivery_id and self.sample_id and self.failurereason):
                    raise ArgumentException(
                        'You need to pass a delivery id, sample id, and a failurereason')
                return self.do_failure(self.action,
                                self.delivery_id,
                                self.sample_id,
                                self.failurereason)

            else:
                if not self.delivery_id:
                    raise ArgumentException('You need to pass a delivery id')
                if not self.sample_id:
                    raise ArgumentException('You need to pass a sample id')
                return self.do_action(self.action, self.delivery_id, self.sample_id)
        except ArgumentException as ex:
            print('Wrong parameters passed:' + ex.message)
            raise ex

    def create(self, delivery_id, sample_id):
        """
        TODO method doc
        :param delivery_id:
        :param sample_id:
        """
        self.params['data'] = json.dumps({'delivery_id': delivery_id},
                                         {'sample_id': sample_id})
        return self.do_http_call('put', self.get_url(delivery_id))

    def get(self, delivery_id):
        """
        TODO method doc
        :param delivery_id:
        """
        r = self.do_http_call('get', self.get_url(delivery_id))
        print(json.dumps(r.json(), indent=4))
        return r


    def get_all(self, by=None):
        """
        TODO method doc
        :param by:
        """
        if by:
            self.params['params'] = {'state': by}
        r = self.do_http_call('get', self.base_url)
        print(json.dumps(r.json(), indent=4))
        return r

    def do_action(self, action, delivery_id, sample_id):
        """
        TODO method doc
        :param action:
        :param delivery_id:
        :param sample_id:
        """
        self.params['data'] = json.dumps({'delivery_id': delivery_id,
                                          'sample_barcode': sample_id})  # this needs to be
                                                                         # sample_barcode for com-
                                                                         # patibility with code
                                                                         # on the Illumina side
        return self.do_http_call('post', self.get_url(delivery_id, 'actions', action))

    def do_failure(self, action, delivery_id, sample_id, failure_reason):
        """
        TODO method doc
        :param action:
        :param delivery_id:
        :param sample_id:
        :param failure_reason:
        """
        self.params['data'] = json.dumps({'delivery_id': delivery_id,
                                          'sample_barcode': sample_id,  # this needs to be
                                                                        # sample_barcode for com-
                                                                        # patibility with code
                                                                        # on the Illumina side
                                          'failure_reason': failure_reason})
        return self.do_http_call('post', self.get_url(delivery_id, 'actions', action))

    def get_url(self, *parts):
        """
        TODO method doc
        :param parts:
        :return:
        """
        return '/'.join([self.base_url] + [part for part in parts]) + '/'

    def do_http_call(self, http_method, url):
        """
        TODO method doc
        :param http_method:
        :param url:
        :return:
        """
        method = getattr(requests, http_method)
        try:
            r = method(url, **self.params)
        except ConnectionError as e:
            print('Connection error. Is the server running?')
            raise e
        print(r.status_code)
        try:
            r.raise_for_status()
        except HTTPError as e:
            print('Response text: {}'.format(r.text))
            raise e
        return r


def main():
    try:
        client = DeliveryAPIClient(get_args())
        client.make_call()
    except KeyboardInterrupt:
        pass


def get_args():
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
python client.py --action=upload_failed --delivery_id=DELIVERY_1 --sample_id=SAMPLE_1 --failurereason='ran out of disk space'
    ''')
    parser.add_argument(
        '--host',
        help='host name and port to connect to, in the format host:port',
        default=os.getenv('GE_API_HOST'))
    parser.add_argument(
        '--user',
        help='which user to authenticate with',
        default=os.getenv('GE_API_USER'))
    parser.add_argument(
        '--action',
        help='which method to call [{}]'.format(', '.join(DeliveryAPIClient.valid_actions)))
    parser.add_argument(
        '--filter', help='status to filter by for --action=all call')
    parser.add_argument(
        '--delivery_id', help='Delivery id for the target delivery')
    parser.add_argument(
        '--sample_id', help='Sample id for the target delivery')
    parser.add_argument(
        '--failurereason',
        help='failure message containing the reason in the case of failure actions')
    args = parser.parse_args()

    host = args.host
    user = args.user
    pswd = os.getenv('GE_API_PASS')

    action = args.action

    if not pswd:
        pswd = getpass.getpass('Password:')

    if not (host and user and pswd and action):
        print(
            'Incorrect parameters provided: host, user, password and action are all required\n')
        parser.print_help()
        sys.exit()
    return dict(args.keyvalues)


if __name__ == '__main__':
    main()