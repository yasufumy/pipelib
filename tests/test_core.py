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

    def check_for_loop(self, data, expected):
        for x, y in zip(data, expected):
            self.assertEqual(x, y)

    def check_correct_pipelined_dataset(self, dataset, data, nested=True):
        self.assertIsInstance(dataset, pipelib.core.PipelinedDataset)
        self.assertEqual(dataset._dataset, data)
        if nested:
            self.assertIsInstance(dataset._func, pipelib.core._NestedFunc)

    def test_apply(self):
        def f(dataset):
            for x in dataset:
                if x % 2 == 0:
                    yield x ** 2

        data = self.data.apply(f)
        expected = f(self.base)

        self.check_for_loop(data, expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_repeat(self):
        data = self.data.repeat()
        expected = list(self.base) * 3

        self.check_for_loop(data, expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_shuffle(self):
        data = self.data.shuffle(100)
        expected = list(self.base)

        self.assertListEqual(sorted(data), expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_batch(self):
        batch_size = 10
        data = self.data.batch(batch_size)
        for x in data:
            self.assertEqual(len(x), batch_size)

        batch_size = 16
        data = self.data.batch(batch_size)
        self.check_for_loop(chain.from_iterable(data), self.base)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_window(self):
        window_size = 3
        data = self.data.window(window_size)
        for x in data:
            self.assertEqual(len(x), window_size)

        expected = zip(*(self.base[i:] for i in range(window_size)))
        self.check_for_loop(data, expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_map(self):
        def f(x):
            return x ** 2

        data = self.data.map(f)

        for x, y in zip(data, self.base):
            self.assertEqual(x, f(y))

        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_filter(self):
        def f(x):
            return x % 2 == 0

        data = self.data.filter(f)
        expected = [y for y in self.base if f(y)]

        self.check_for_loop(data, expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_flat_map(self):
        repeat = 3

        def f(x):
            return [x for _ in range(repeat)]

        data = self.data.flat_map(f)

        expected = [x for x in self.base for _ in range(repeat)]

        self.check_for_loop(data, expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_map_parallel(self):
        def f(x):
            return x ** 2

        data = self.data.map_parallel(f)
        result = [x for x in data]
        result.sort()
        expected = [f(x) for x in self.base]

        self.assertListEqual(result, expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_filter_parallel(self):
        def f(x):
            return x % 2 == 0

        data = self.data.filter_parallel(f)
        result = [x for x in data]
        result.sort()
        expected = [y for y in self.base if f(y)]

        self.assertListEqual(result, expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_flat_map_parallel(self):
        repeat = 3

        def f(x):
            return [x for _ in range(repeat)]

        data = self.data.flat_map_parallel(f)
        result = [x for x in data]
        result.sort()
        expected = [i for x in self.base for i in f(x)]

        self.assertListEqual(result, expected)
        self.check_correct_pipelined_dataset(data, self.base, nested=False)

    def test_zip(self):
        data1 = self.data.map(lambda x: x ** 2)
        data2 = self.data.map(lambda x: x / 2)

        data = data1.zip(data2)

        for x, y in zip(data, self.base):
            self.assertEqual(x[0], y ** 2)
            self.assertEqual(x[1], y / 2)

        self.check_correct_pipelined_dataset(data, self.base)

    def test_concat(self):
        data1 = self.data.map(lambda x: x ** 2)
        data2 = self.data.map(lambda x: x / 2)

        data = data1.concat(data2)
        expected = [x ** 2 for x in self.base] + [x / 2 for x in self.base]

        self.check_for_loop(data, expected)

        self.check_correct_pipelined_dataset(data, self.base)

    def test_method_chain(self):
        data = self.data.map(lambda x: x ** 2) \
            .filter(lambda x: x % 2 == 0) \
            .flat_map(lambda x: [x, x]) \
            .map(lambda x: x / 2) \
            .filter(lambda x: x < 100) \
            .flat_map(lambda x: [x, x, x])

        expected = [x ** 2 for x in self.base if (x ** 2) % 2 == 0]
        expected = [[x / 2] * 6 for x in expected if (x / 2) < 100]

        self.check_for_loop(data, chain.from_iterable(expected))
        self.check_correct_pipelined_dataset(data, self.base)

    def test_get_prefetch_iterator(self):
        it = self.data.get_prefetch_iterator(n_prefetch=5)
        for x, y in zip(it, self.base):
            self.assertEqual(x, y)

    def test_all(self):
        data = self.data
        expected = list(self.base)

        self.assertListEqual(data.all(), expected)

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
        self.check_correct_pipelined_dataset(data, self.base)

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
        self.assertEqual(data._dataset, list(self.base))


class TextDatasetTestCase(TestCase):
    def test_text(self):
        lines = ['This is a test .', 'That is also a test .']
        fp = tempfile.NamedTemporaryFile()
        for x in lines:
            fp.write(f'{x}\n'.encode('utf-8'))
        fp.seek(0)

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
        from pathlib import Path
        expected = []
        dirpath = Path(tempdir.name)
        for i in range(10):
            filename = f'{i:03d}.txt'
            (dirpath / filename).touch()
            expected.append(filename)

        data = DirDataset(tempdir.name, pattern='*.txt')
        for x, y in zip(sorted(data.all()), expected):
            self.assertEqual(x, f'{tempdir.name}/{y}')

        self.assertIsInstance(data._dataset, pipelib.core._Repeated)

        data = data.map(lambda x: x.title()) \
            .filter(lambda x: True)\
            .flat_map(lambda x: [x])

        for x, y in zip(sorted(data.all()), expected):
            self.assertEqual(x, f'{tempdir.name}/{y}'.title())

        self.assertIsInstance(data, pipelib.core.PipelinedDataset)
        self.assertIsInstance(data._dataset, pipelib.core._Repeated)
        self.assertIsInstance(data._func, pipelib.core._NestedFunc)

        tempdir.cleanup()
