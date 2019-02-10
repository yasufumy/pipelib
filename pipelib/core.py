import random
import pickle
from pathlib import Path
from itertools import chain, islice, tee
from collections import deque

import pipelib
from pipelib import parallel
from pipelib import iterators


class Dataset:
    def __init__(self, dataset):
        if isinstance(dataset, pipelib.Dataset):
            self._dataset = dataset._dataset
        else:
            self._dataset = dataset

    def __iter__(self):
        yield from self._dataset

    def get_prefetch_iterator(self, n_prefetch=1):
        return iterators.PrefetchIterator(self, n_prefetch)

    def apply(self, func):
        return PipelinedDataset(self, func)

    def repeat(self):
        def f(dataset):
            while True:
                yield from dataset
        return PipelinedDataset(self, f)

    def batch(self, batch_size):
        def f(dataset):
            iterator = iter(dataset)
            yield from iter(lambda: list(islice(iterator, batch_size)), [])
        return PipelinedDataset(self, f)

    def shuffle(self, shuffle_size):
        def f(dataset):
            iterator = iter(dataset)
            for chunk in iter(lambda: list(islice(iterator, shuffle_size)), []):
                random.shuffle(chunk)
                yield from chunk
        return PipelinedDataset(self, f)

    def window(self, window_size):
        def f(dataset):
            yield from zip(*(deque(islice(it, i), 0) or it
                             for i, it in enumerate(tee(dataset, window_size))))
        return PipelinedDataset(self, f)

    def map(self, map_func):
        def f(dataset):
            return map(map_func, dataset)
        return PipelinedDataset(self, f)

    def flat_map(self, map_func):
        def f(dataset):
            return chain.from_iterable(map(map_func, dataset))
        return PipelinedDataset(self, f)

    def filter(self, predicate):
        def f(dataset):
            return filter(predicate, dataset)
        return PipelinedDataset(self, f)

    def zip(self, *others):
        assert all(isinstance(other, pipelib.Dataset) for other in others)

        def f(dataset):
            yield from zip(dataset, *others)
        return PipelinedDataset(self, f)

    def concat(self, *others):
        assert all(isinstance(other, pipelib.Dataset) for other in others)

        def f(dataset):
            yield from chain(dataset, *others)
        return PipelinedDataset(self, f)

    def map_parallel(self, map_func, n=None, chunksize=1):
        return PipelinedDataset(
            self, parallel.MapParallel(map_func, n, chunksize))

    def flat_map_parallel(self, map_func, n=None, chunksize=1):
        return PipelinedDataset(
            self, parallel.FlatMapParallel(map_func, n, chunksize))

    def filter_parallel(self, predicate, n=None, chunksize=1):
        return PipelinedDataset(
            self, parallel.FilterParallel(predicate, n, chunksize))

    def all(self):
        return list(self)

    def take(self, n):
        return list(islice(self, n))

    def first(self):
        return next(iter(self))

    def save(self, filename):
        evaluated_dataset = list(self)
        with open(filename, 'wb') as f:
            pickle.dump(evaluated_dataset, f)
        return self

    @staticmethod
    def load(filename):
        with open(filename, 'rb') as f:
            dataset = pickle.load(f)
        return Dataset(dataset)


class _NestedFunc:
    __slots__ = ['_prev_func', '_func']

    def __init__(self, prev_func, func):
        self._prev_func = prev_func
        self._func = func

    def _flatten_func(self, func):
        if isinstance(func, _NestedFunc):
            yield from self._flatten_func(func._prev_func)
            yield from self._flatten_func(func._func)
        else:
            yield func

    def __call__(self, dataset):
        for func in self._flatten_func(self):
            dataset = func(dataset)
        return dataset


class PipelinedDataset(Dataset):

    def __init__(self, dataset, func):
        if not isinstance(dataset, pipelib.core.PipelinedDataset):
            self._func = func
        else:
            self._func = _NestedFunc(dataset._func, func)

        super().__init__(dataset)

    def __iter__(self):
        yield from self._func(self._dataset)


class _Repeated:
    __slots__ = ['_generator', '_args', '_kwargs']

    def __init__(self, generator, *args, **kwargs):
        self._generator = generator
        self._args = args
        self._kwargs = kwargs

    def __iter__(self):
        return self._generator(*self._args, **self._kwargs)


class TextDataset(Dataset):
    def __init__(self, filepath, encoding='utf-8'):
        filepath = Path(filepath)
        assert filepath.is_file()

        self._filepath = filepath
        self._encoding = encoding

    @property
    def _dataset(self):
        def g(filepath, encoding):
            with filepath.open(encoding=encoding) as f:
                for line in f:
                    yield line.rstrip()
        return _Repeated(g, filepath=self._filepath, encoding=self._encoding)


class DirDataset(Dataset):
    def __init__(self, dirpath, pattern='*'):
        dirpath = Path(dirpath)
        assert dirpath.is_dir()

        self._dirpath = dirpath
        self._pattern = pattern

    @property
    def _dataset(self):
        def g(dirpath, pattern):
            for path in dirpath.glob(pattern):
                yield str(path)
        return _Repeated(g, dirpath=self._dirpath, pattern=self._pattern)
