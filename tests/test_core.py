from unittest import TestCase
from unittest.mock import patch, Mock
import tempfile
from itertools import chain

import pipelib
from pipelib import Dataset, TextDataset, DirDataset


class DatasetTestCase(TestCase):

    def setUp(self):
        self.base = range(100)
        self.data = Dataset(self.base)

    def test_apply(self):
        def f(dataset):
            for x in dataset:
                if x % 2 == 0:
                    yield x ** 2

        data = self.data.apply(f)
        expected = f(self.base)

        for x, y in zip(data, expected):
            self.assertEqual(x, y)

    def test_repeat(self):
        data = self.data.repeat()
        expected = list(self.base) * 3

        for x, y in zip(data, expected):
            self.assertEqual(x, y)

    def test_shuffle(self):
        data = self.data.shuffle(100).all()
        data.sort()
        expected = list(self.base)

        self.assertListEqual(data, expected)

    def test_batch(self):
        batch_size = 10
        data = self.data.batch(batch_size)
        for x in data:
            self.assertEqual(len(x), batch_size)

        batch_size = 16
        data = self.data.batch(batch_size)
        for x, y in zip(chain.from_iterable(data), self.base):
            self.assertEqual(x, y)

    def test_map(self):
        def f(x):
            return x ** 2

        data = self.data.map(f)

        for x, y in zip(data, self.base):
            self.assertEqual(x, f(y))

    def test_filter(self):
        def f(x):
            return x % 2 == 0

        data = self.data.filter(f)
        expected = [y for y in self.base if f(y)]

        for x, y in zip(data, expected):
            self.assertEqual(x, y)

    def test_flat_map(self):
        repeat = 3

        def f(x):
            return [x for _ in range(repeat)]

        data = self.data.flat_map(f)

        expected = [x for x in self.base for _ in range(repeat)]

        for x, y in zip(data, expected):
            self.assertEqual(x, y)

    def test_zip(self):
        data1 = self.data.map(lambda x: x ** 2)
        data2 = self.data.map(lambda x: x / 2)

        data = data1.zip(data2)

        for x, y in zip(data, self.base):
            self.assertEqual(x[0], y ** 2)
            self.assertEqual(x[1], y / 2)

        self.assertIsInstance(data, pipelib.core.PipelinedDataset)
        self.assertEqual(data._dataset, self.base)
        self.assertIsInstance(data._func, pipelib.core._NestedFunc)

    def test_method_chain(self):
        data = self.data.map(lambda x: x ** 2) \
            .filter(lambda x: x % 2 == 0) \
            .flat_map(lambda x: [x, x]) \
            .map(lambda x: x / 2) \
            .filter(lambda x: x < 100) \
            .flat_map(lambda x: [x, x, x])

        expected = [x ** 2 for x in self.base if (x ** 2) % 2 == 0]
        expected = [[x / 2] * 6 for x in expected if (x / 2) < 100]

        for x, y in zip(data, chain.from_iterable(expected)):
            self.assertEqual(x, y)

        self.assertIsInstance(data, pipelib.core.PipelinedDataset)
        self.assertEqual(data._dataset, self.base)
        self.assertIsInstance(data._func, pipelib.core._NestedFunc)

    def test_all(self):
        data = self.data.all()
        expected = list(self.base)

        self.assertListEqual(data, expected)

    def test_first(self):
        data = self.data.first()
        expected = next(iter(self.base))

        self.assertEqual(data, expected)

    def test_take(self):
        n = 50
        data = self.data.take(n)
        expected = list(self.base[:n])

        self.assertListEqual(data, expected)

    @patch('pipelib.core.open')
    @patch('pipelib.core.pickle.dump')
    def test_save(self, pickle_dump_mock, open_mock):
        enter_mock = Mock()
        # mock file object
        open_mock.return_value.__enter__.return_value = enter_mock

        filepath = '/path/to/dataset'
        data = self.data.filter(lambda x: x % 2 == 0) \
            .map(lambda x: x ** 2) \
            .save(filepath)
        open_mock.assert_called_once_with(filepath, 'wb')
        pickle_dump_mock.assert_called_once_with(
            data.all(), enter_mock)

        expected = [x ** 2 for x in self.base if x % 2 == 0]
        self.assertListEqual(data.all(), expected)

    @patch('pipelib.core.open')
    @patch('pipelib.core.pickle.load')
    def test_load(self, pickle_load_mock, open_mock):
        pickle_load_mock.return_value = list(self.base)
        enter_mock = Mock()
        open_mock.return_value.__enter__.return_value = enter_mock

        filepath = '/path/to/dataset'
        data = Dataset.load(filepath)
        open_mock.assert_called_once_with(filepath, 'rb')
        pickle_load_mock.assert_called_once_with(enter_mock)

        self.assertListEqual(data.all(), list(self.base))


class TextDatasetTestCase(TestCase):
    def test_text(self):
        lines = ['This is a test .', 'That is also a test .']
        fp = tempfile.NamedTemporaryFile()
        for x in lines:
            fp.write(f'{x}\n'.encode('utf-8'))

        data = TextDataset(fp.name)
        for x, y in zip(data, lines):
            self.assertEqual(x, y)

        self.assertIsInstance(data._dataset, pipelib.core._Repeated)

        data = data.map(str.split).filter(lambda x: True).flat_map(lambda x: x)

        for x, y in zip(data, chain.from_iterable([l.split() for l in lines])):
            self.assertEqual(x, y)

        self.assertIsInstance(data, pipelib.core.PipelinedDataset)
        self.assertIsInstance(data._dataset, pipelib.core._Repeated)
        self.assertIsInstance(data._func, pipelib.core._NestedFunc)

        fp.close()


class DirDatasetTestCase(TestCase):
    def test_directory(self):
        tempdir = tempfile.TemporaryDirectory()
        expected = [tempfile.NamedTemporaryFile(dir=tempdir.name).name
                    for _ in range(2)]

        data = DirDataset(tempdir.name)
        for x, y in zip(data, expected):
            self.assertEqual(x, y)

        self.assertIsInstance(data._dataset, pipelib.core._Repeated)

        data = data.map(lambda x: x.title()) \
            .filter(lambda x: True)\
            .flat_map(lambda x: [x])

        for x, y in zip(data, expected):
            self.assertEqual(x, y.title())

        self.assertIsInstance(data, pipelib.core.PipelinedDataset)
        self.assertIsInstance(data._dataset, pipelib.core._Repeated)
        self.assertIsInstance(data._func, pipelib.core._NestedFunc)

        tempdir.cleanup()
