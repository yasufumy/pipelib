from unittest import TestCase

from pipelib import parallel


class ParallelTestCase(TestCase):

    def setUp(self):
        self.data = range(100)

    def test_map_parallel(self):
        result = list(parallel.MapParallel(lambda x: x ** 2)(self.data))
        result.sort()
        expected = [x ** 2 for x in self.data]
        self.assertListEqual(result, expected)

    def test_filter_parallel(self):
        def predicate(x):
            return x % 2 == 0

        result = list(parallel.FilterParallel(predicate)(self.data))
        result.sort()
        task = parallel.FilterParallel._FilterTask(predicate)
        expected = [task(x) for x in self.data]
        expected = [x[0] for x in expected if x[1]]
        self.assertListEqual(result, expected)

    def test_flat_map_parallel(self):
        result = list(parallel.FlatMapParallel(lambda x: [x])(self.data))
        result.sort()
        expected = [x for x in self.data]
        self.assertListEqual(result, expected)
