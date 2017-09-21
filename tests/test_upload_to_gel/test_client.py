import hashlib
from unittest.mock import patch, Mock

import pytest

from tests import TestProjectManagement
from upload_to_gel.client import DeliveryAPIClient, ArgumentException

json_data={
    "state": "awaiting_delivery",
    "delivery_id": "id1",
}
fake_get_response = Mock(status_code=200, json=Mock(return_value=json_data))
fake_post_response = Mock(status_code=201)
class TestDeliveryAPIClient(TestProjectManagement):

    def create_client(self, **kwargs):
        return DeliveryAPIClient('test.server', 'username', 'password', **kwargs)

    def test_make_call(self):
        c = self.create_client(action='unknown', delivery_id='id1')
        with pytest.raises(ArgumentException):
            c.make_call()

        c = self.create_client(action='get', delivery_id='id1')
        with patch('requests.get', return_value=fake_get_response) as mocked_get:
            request = c.make_call()
            assert request.json()['state'] == 'awaiting_delivery'
            assert mocked_get.call_args[0][0] == 'http://test.server/api/deliveries/id1/'

        c = self.create_client(action='create', delivery_id='id1', sample_id='s1')
        # FIXME: Why do we need a sample id ??
        with patch('requests.put', return_value=fake_post_response) as mocked_put:
            request = c.make_call()
            assert mocked_put.call_args[0][0] == 'http://test.server/api/deliveries/id1/'
