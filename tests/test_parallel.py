from unittest import TestCase

from pipelib import parallel


class ParallelTestCase(TestCase):

    def setUp(self):
        self.data = range(100)

    def test_map_parallel(self):
        expected = [x ** 2 for x in self.data]
        # ordered
        result = parallel.MapParallel(lambda x: x ** 2)(self.data)
        for x, y in zip(result, expected):
            self.assertEqual(x, y)
        # unordered
        result = list(parallel.MapParallel(
            lambda x: x ** 2, unordered=True)(self.data))
        result.sort()
        self.assertListEqual(result, expected)

    def test_filter_parallel(self):
        def predicate(x):
            return x % 2 == 0

        task = parallel.FilterParallel._FilterTask(predicate)
        expected = [task(x)[0] for x in self.data if task(x)[1]]

        # ordered
        result = parallel.FilterParallel(predicate)(self.data)
        for x, y in zip(result, expected):
            self.assertEqual(x, y)

        # unordered
        result = list(parallel.FilterParallel(
            predicate, unordered=True)(self.data))
        result.sort()
        self.assertListEqual(result, expected)

    def test_flat_map_parallel(self):
        expected = [x for x in self.data]

        # ordered
        result = parallel.FlatMapParallel(lambda x: [x])(self.data)
        for x, y in zip(result, expected):
            self.assertEqual(x, y)

        # unordered
        result = list(parallel.FlatMapParallel(
            lambda x: [x], unordered=True)(self.data))
        result.sort()
        self.assertListEqual(result, expected)
