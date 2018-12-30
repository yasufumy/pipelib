from functools import partial
try:
    import numpy as np
except ModuleNotFoundError:
    print('please install numpy.')

from pipelib import Dataset


if __name__ == '__main__':
    data = Dataset(range(100))

    print(data.map(partial(np.array, dtype=np.int32)).take(5))
    print(data.map(partial(np.array, dtype=np.float32)).take(5))
