from unittest import TestCase
from unittest.mock import patch, Mock

from pipelib import serializers
from pipelib import Dataset


class SerializersTestCase(TestCase):
    def setUp(self):
        self.data = Dataset(range(100)) \
            .map(lambda x: x ** 2) \
            .map(lambda x: x / 2)

    @patch('pipelib.serializers.open')
    @patch('pipelib.serializers.cloudpickle.dump')
    def test_save_pipeline(self, cloudpickle_dump_mock, open_mock):
        enter_mock = Mock()
        open_mock.return_value.__enter__.return_value = enter_mock

        filepath = '/path/to/pipeline'
        serializers.save_pipeline(filepath, self.data)

        open_mock.assert_called_once_with(filepath, 'wb')
        cloudpickle_dump_mock.assert_called_once_with(
            self.data._func, enter_mock)

    @patch('pipelib.serializers.Path')
    @patch('pipelib.serializers.cloudpickle.load')
    def test_load_pipeline(self, cloudpickle_load_mock, PathMock):
        cloudpickle_load_mock.return_value = self.data._func
        enter_mock = Mock()
        PathMock.return_value.open.return_value.__enter__.return_value = enter_mock
        filepath = '/path/to/pipeline'
        pipeline = serializers.load_pipeline(filepath)

        PathMock.assert_called_once_with(filepath)
        PathMock.return_value.open.assert_called_once_with('rb')
        cloudpickle_load_mock.assert_called_once_with(enter_mock)
        self.assertEqual(pipeline, self.data._func)
