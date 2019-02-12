from itertools import chain

import multiprocess


class MapParallel:
    def __init__(self, func, n=None, chunksize=1, unordered=False):
        self._func = func
        self._n = n
        self._chunksize = chunksize
        if not unordered:
            self._map_method = 'imap'
        else:
            self._map_method = 'imap_unordered'

    def __call__(self, dataset):
        with multiprocess.Pool(self._n) as p:
            yield from getattr(p, self._map_method)(
                self._func, dataset, self._chunksize)


class FlatMapParallel(MapParallel):
    def __call__(self, dataset):
        with multiprocess.Pool(self._n) as p:
            yield from chain.from_iterable(
                getattr(p, self._map_method)(
                    self._func, dataset, self._chunksize))


class FilterParallel(MapParallel):

    class _FilterTask:
        __slots__ = ['_predicate']

        def __init__(self, predicate):
            self._predicate = predicate

        def __call__(self, x):
            return x, self._predicate(x)

    def __call__(self, dataset):
        task = self._FilterTask(self._func)

        with multiprocess.Pool(self._n) as p:
            yield from (x for x, keep in
                        getattr(p, self._map_method)(
                            task, dataset, self._chunksize) if keep)
