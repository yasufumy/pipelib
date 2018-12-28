from unittest import TestCase
import tempfile

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

    def test_method_chain(self):
        data = self.data.filter(lambda x: x % 2 == 0).map(lambda x: x ** 2)
        expected = [x ** 2 for x in self.base if x % 2 == 0]

        for x, y in zip(data, expected):
            self.assertEqual(x, y)

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


class TextDatasetTestCase(TestCase):
    def test_text(self):
        lines = ['This is a test .', 'That is also a test .']
        fp = tempfile.NamedTemporaryFile()
        for x in lines:
            fp.write(f'{x}\n'.encode('utf-8'))

        data = TextDataset(fp.name)
        for x, y in zip(data, lines):
            self.assertEqual(x, y)

        fp.close()


class DirDatasetTestCase(TestCase):
    def test_directory(self):
        tempdir = tempfile.TemporaryDirectory()
        expected = [tempfile.NamedTemporaryFile(dir=tempdir.name).name
                    for _ in range(2)]

        data = DirDataset(tempdir.name)
        for x, y in zip(data, expected):
            self.assertEqual(x, y)

        tempdir.cleanup()
