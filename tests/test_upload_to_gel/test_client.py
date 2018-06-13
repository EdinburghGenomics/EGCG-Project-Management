import os
import pytest
from unittest.mock import patch, Mock
from tests import TestProjectManagement
from upload_to_gel.client import DeliveryAPIClient, ArgumentException, get_args

fake_get_response = Mock(status_code=200, json=Mock(return_value={'state': 'awaiting_delivery', 'delivery_id': 'id1'}))
fake_post_response = Mock(status_code=201)


class TestDeliveryAPIClient(TestProjectManagement):
    @staticmethod
    def create_client(**kwargs):
        return DeliveryAPIClient('test.server', 'username', 'password', **kwargs)

    def test_make_call(self):
        with pytest.raises(ArgumentException) as e:
            c = self.create_client(action='unknown', delivery_id='id1')
            c.make_call()
        assert 'Action specified is not valid' in str(e.value)

        c = self.create_client(action='get', delivery_id='id1')
        with patch('requests.get', return_value=fake_get_response) as mocked_get:
            c.make_call()
            assert mocked_get.call_args[0][0] == 'http://test.server/api/deliveries/id1/'

        c = self.create_client(action='create', delivery_id='id1')
        with patch('requests.put') as mocked_put:
            c.make_call()
            mocked_put.assert_called_once_with(
                'http://test.server/api/deliveries/id1/',
                auth=c.auth,
                json={'delivery_id': 'id1'}
            )
            assert mocked_put.call_args[0][0] == 'http://test.server/api/deliveries/id1/'

        c = self.create_client(action='upload_failed', delivery_id='id1', sample_id='s1', failure_reason='having a bad day')
        with patch('requests.post') as mocked_post:
            c.make_call()
            mocked_post.assert_called_once_with(
                'http://test.server/api/deliveries/id1/actions/upload_failed/',
                auth=c.auth,
                json={'sample_barcode': 's1', 'delivery_id': 'id1', 'failure_reason': 'having a bad day'}
            )

        c = self.create_client(action='delivered', delivery_id='id1', sample_id='s1')
        with patch('requests.post') as mocked_post:
            c.make_call()
            mocked_post.assert_called_once_with(
                'http://test.server/api/deliveries/id1/actions/delivered/',
                auth=c.auth,
                json={'sample_barcode': 's1', 'delivery_id': 'id1'}
            )

        c = self.create_client(action='all', filter='a_state')
        with patch('requests.get', return_value=fake_get_response) as mocked_get:
            request = c.make_call()
            assert request.json()['state'] == 'awaiting_delivery'
            assert mocked_get.call_args[0][0] == 'http://test.server/api/deliveries'
            assert mocked_get.call_args[1]['params'] == {'state': 'a_state'}

    def test_get_args(self):
        os.environ['GE_API_PASS'] = 'a_password'
        argv = '--host h --user u --action a --filter f --delivery_id d --sample_id s'.split()
        obs = get_args(argv)
        assert obs == {'host': 'h', 'user': 'u', 'action': 'a', 'filter': 'f', 'delivery_id': 'd', 'sample_id': 's',
                       'failure_reason': None, 'pswd': 'a_password'}

        del os.environ['GE_API_PASS']
