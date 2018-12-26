from itertools import chain, islice
from pathlib import Path


class Dataset:
    def __init__(self, dataset):
        if isinstance(dataset, Dataset):
            self._dataset = dataset._dataset
        else:
            self._dataset = dataset

    def __iter__(self):
        yield from self._dataset

    def apply(self, func):
        return PipelinedDataset(self, func)

    def map(self, map_func):
        def f(dataset):
            return map(map_func, dataset)
        return PipelinedDataset(self, f)

    def flat_map(self, flat_map_func):
        def f(dataset):
            return chain.from_iterable(map(flat_map_func, dataset))
        return PipelinedDataset(self, f)

    def filter(self, predicate):
        def f(dataset):
            return filter(predicate, dataset)
        return PipelinedDataset(self, f)

    def collect(self):
        return list(iter(self))

    def take(self, n):
        return list(islice(self, n))

    def first(self):
        return next(iter(self))


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
        if not isinstance(dataset, PipelinedDataset):
            self._func = func
        else:
            self._func = _NestedFunc(dataset._func, func)

        super().__init__(dataset)

    def __iter__(self):
        yield from self._func(self._dataset)


class TextDataset(Dataset):
    def __init__(self, filepath, encoding='utf-8'):
        filepath = Path(filepath)
        assert filepath.is_file()

        self._filepath = filepath
        self._encoding = encoding

    @property
    def _dataset(self):
        with self._filepath.open(encoding=self._encoding) as f:
            for line in f:
                yield line[:-1]


class DirDataset(Dataset):
    def __init__(self, dirpath, pattern='*'):
        dirpath = Path(dirpath)
        assert dirpath.is_dir()

        self._dirpath = dirpath
        self._pattern = pattern

    @property
    def _dataset(self):
        for path in self._dirpath.glob(self._pattern):
            yield str(path)
