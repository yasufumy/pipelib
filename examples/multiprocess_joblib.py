import os
import time
try:
    from joblib import Parallel, delayed
except ModuleNotFoundError:
    print('please install joblib.')
from itertools import chain


class MapParallel:
    def __init__(self, func, n=-1, chunksize=1):
        self._func = func
        self._n = n
        self._chunksize = chunksize

    def __call__(self, dataset):
        yield from Parallel(n_jobs=self._n)(delayed(self._func)(x) for x in dataset)


class FlatMapParallel(MapParallel):
    def __call__(self, dataset):
        yield from chain.from_iterable(
            Parallel(n_jobs=self._n)(delayed(self._func)(x) for x in dataset))


class FilterParallel(MapParallel):
    def __call__(self, dataset):
        yield from (x for x, keep in Parallel(n_jobs=self._n)(
            delayed(lambda x: (x, self._func(x)))(x) for x in dataset) if keep)


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
