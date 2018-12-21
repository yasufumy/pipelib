from itertools import chain, islice


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
