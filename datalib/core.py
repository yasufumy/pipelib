from itertools import chain, islice
from functools import wraps
from pathlib import Path


class Dataset:
    def __init__(self, data):
        self._data = data

    def __iter__(self):
        yield from self._data

    def map(self, map_func):
        def f(data):
            return map(map_func, data)
        return PipelinedDataset(self, f)

    def flat_map(self, flat_map_func):
        def f(data):
            return chain.from_iterable(map(flat_map_func, data))
        return PipelinedDataset(self, f)

    def filter(self, predicate):
        def f(data):
            return filter(predicate, data)
        return PipelinedDataset(self, f)

    def collect(self):
        return list(iter(self))

    def take(self, n):
        return list(islice(self, n))

    def first(self):
        return next(iter(self))


class PipelinedDataset(Dataset):
    def __init__(self, dataset, func):
        if not isinstance(dataset, PipelinedDataset):
            self._func = func
        else:
            prev_func = dataset._func

            def nested_func(data):
                return func(prev_func(data))

            self._func = nested_func

        self._data = dataset._data

    def __iter__(self):
        yield from self._func(self._data)


class _Repeat:
    def __init__(self, func, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._func = func

    def __iter__(self):
        return self._func(*self._args, **self._kwargs)


def _repeated(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return _Repeat(func, *args, **kwargs)
    return wrapper


def text(filepath, encoding='utf-8'):
    filepath = Path(filepath)

    assert filepath.is_file()

    @_repeated
    def i():
        with filepath.open(encoding=encoding) as f:
            for line in f:
                yield line[:-1]

    return Dataset(i())


def directory(dirpath, pattern='*'):
    dirpath = Path(dirpath)

    assert dirpath.is_dir()

    @_repeated
    def i():
        for path in dirpath.glob(pattern):
            yield str(path)

    return Dataset(i())
