import os
import time
import multiprocessing
from itertools import chain


class MapParallel:
    def __init__(self, func, n=None, chunksize=1):
        self._func = func
        self._n = n
        self._chunksize = chunksize

    def __call__(self, dataset):
        with multiprocessing.Pool(self._n) as p:
            yield from p.imap_unordered(self._func, dataset, self._chunksize)


class FlatMapParallel(MapParallel):
    def __call__(self, dataset):
        with multiprocessing.Pool(self._n) as p:
            yield from chain.from_iterable(
                p.imap_unordered(self._func, dataset, self._chunksize))


class FilterParallel(MapParallel):

    class _FilterTask:
        __slots__ = ['_func']

        def __init__(self, func):
            self._func = func

        def __call__(self, x):
            return x, self._func(x)

    def __call__(self, dataset):
        task = self._FilterTask(self._func)
        with multiprocessing.Pool(self._n) as p:
            yield from (
                x for x, keep in p.imap_unordered(task, dataset, self._chunksize) if keep)


def slow_map(x):
    print(f'[{os.getpid()}]: I\'m slow map func...')
    time.sleep(0.5)
    return x


def slow_filter(x):
    print(f'[{os.getpid()}]: I\'m slow filter func...')
    time.sleep(0.5)
    return True


def slow_flat_map(x):
    print(f'[{os.getpid()}]: I\'m slow flat map func...')
    time.sleep(0.5)
    return [x]


if __name__ == '__main__':
    from pipelib import Dataset
    data = Dataset(range(100))

    start = time.time()
    data.map(slow_map).filter(slow_filter).flat_map(slow_flat_map).all()
    normal_time = time.time() - start

    start = time.time()
    data.apply(MapParallel(slow_map)) \
        .apply(FilterParallel(slow_filter)) \
        .apply(FlatMapParallel(slow_flat_map)).all()
    faster_time = time.time() - start

    print(normal_time, faster_time)
